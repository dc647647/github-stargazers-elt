import logging
import os
import re
import time
import requests
from datetime import datetime, timezone
from pathlib import Path

import shutil

import dlt
import duckdb
from dotenv import load_dotenv

load_dotenv()

log = logging.getLogger(__name__)

REPOS = [
    "dbt-labs/dbt-core",
    "apache/airflow",
    "dagster-io/dagster",
    "duckdb/duckdb",
    "dlt-hub/dlt",
]

# Each repo loads into its own DuckDB table — no write contention between tasks
REPO_TABLE_MAP = {
    "dbt-labs/dbt-core": "raw_dbt_core",
    "apache/airflow":    "raw_airflow",
    "dagster-io/dagster": "raw_dagster",
    "duckdb/duckdb":     "raw_duckdb",
    "dlt-hub/dlt":       "raw_dlt",
}

DUCKDB_PATH = os.getenv(
    "DUCKDB_PATH",
    str(Path(__file__).parent.parent / "data" / "stargazers.duckdb"),
)

# Per-repo GitHub tokens — each has its own 5,000 req/hr budget.
# Falls back to GITHUB_TOKEN if a repo-specific token is not set.
REPO_TOKEN_MAP = {
    "dbt-labs/dbt-core":  os.getenv("GITHUB_TOKEN_DBT_CORE",  os.getenv("GITHUB_TOKEN")),
    "apache/airflow":     os.getenv("GITHUB_TOKEN_AIRFLOW",   os.getenv("GITHUB_TOKEN")),
    "dagster-io/dagster": os.getenv("GITHUB_TOKEN_DAGSTER",   os.getenv("GITHUB_TOKEN")),
    "duckdb/duckdb":      os.getenv("GITHUB_TOKEN_DUCKDB",    os.getenv("GITHUB_TOKEN")),
    "dlt-hub/dlt":        os.getenv("GITHUB_TOKEN_DLT",       os.getenv("GITHUB_TOKEN")),
}

# GitHub caps stargazer results at 40,000 (400 pages × 100)
MAX_PAGES = 400


def _make_session(repo: str) -> requests.Session:
    session = requests.Session()
    session.headers.update(
        {
            "Accept": "application/vnd.github.star+json",
            "X-GitHub-Api-Version": "2022-11-28",
        }
    )
    token = REPO_TOKEN_MAP.get(repo)
    if token:
        session.headers["Authorization"] = f"Bearer {token}"
    else:
        log.warning("[%s] No GitHub token set. Unauthenticated requests are limited to 60/hr.", repo)
    return session


def _parse_last_page(link_header: str) -> int:
    """Extract the last page number from a GitHub Link response header."""
    match = re.search(r'[?&]page=(\d+)>; rel="last"', link_header)
    return int(match.group(1)) if match else 1


def _fetch_page(session: requests.Session, repo: str, page: int) -> tuple[int, list[dict]]:
    """Fetch a single page of stargazers. Retries automatically on rate limits."""
    url = f"https://api.github.com/repos/{repo}/stargazers"
    while True:
        response = session.get(url, params={"per_page": 100, "page": page}, timeout=30)

        if response.status_code in (403, 429):
            reset_ts = int(response.headers.get("X-RateLimit-Reset", time.time() + 60))
            wait_secs = max(reset_ts - time.time(), 0) + 5
            log.warning("[%s] Rate limited on page %d. Waiting %.0fs...", repo, page, wait_secs)
            time.sleep(wait_secs)
            continue

        # GitHub's 40k cap — no more pages available
        if response.status_code == 422:
            return page, []

        response.raise_for_status()
        data = response.json()
        extracted_at = datetime.now(timezone.utc)

        records = [
            {
                "user_login":   item["user"]["login"],
                "user_id":      item["user"]["id"],
                "repo":         repo,
                "starred_at":   datetime.fromisoformat(item["starred_at"].replace("Z", "+00:00"))
                                if item.get("starred_at") else None,
                "avatar_url":   item["user"]["avatar_url"],
                "html_url":     item["user"]["html_url"],
                "extracted_at": extracted_at,
            }
            for item in data
        ]
        return page, records


def get_stargazers(repo: str) -> list[dict]:
    """Fetch all stargazers for a repo, page by page."""
    session = _make_session(repo)

    _, first_records = _fetch_page(session, repo, 1)
    if not first_records:
        return []

    probe = session.get(
        f"https://api.github.com/repos/{repo}/stargazers",
        params={"per_page": 100, "page": 1},
        timeout=30,
    )
    last_page = min(_parse_last_page(probe.headers.get("Link", "")), MAX_PAGES)
    log.info("[%s] %d pages to fetch (~%s records)", repo, last_page, f"{last_page * 100:,}")

    stargazers = list(first_records)
    for page in range(2, last_page + 1):
        _, records = _fetch_page(session, repo, page)
        if not records:
            break
        stargazers.extend(records)
        if page % 50 == 0:
            log.info("[%s] ...%d/%d pages done (%s records so far)", repo, page, last_page, f"{len(stargazers):,}")

    return stargazers


def load_with_dlt(records: list[dict], repo: str) -> None:
    """
    Load stargazer records into DuckDB via dlt.
    dlt handles schema inference, table creation, and full-refresh (replace).
    Retries with backoff when another task holds the DuckDB file lock.
    """
    table_name = REPO_TABLE_MAP[repo]

    pipeline_name = f"stargazers_{table_name}"

    # Clear any stuck pipeline state from previous failed runs
    pipeline_dir = Path.home() / ".dlt" / "pipelines" / pipeline_name
    shutil.rmtree(pipeline_dir, ignore_errors=True)

    def _records_gen():
        yield from records

    max_retries = 12
    for attempt in range(max_retries):
        try:
            # Full refresh: drop the table so dlt creates it fresh with its schema.
            # We use append disposition since the table is empty after the drop.
            con = duckdb.connect(DUCKDB_PATH)
            con.execute(f"DROP TABLE IF EXISTS {table_name}")
            con.close()

            pipeline = dlt.pipeline(
                pipeline_name=pipeline_name,
                destination=dlt.destinations.duckdb(credentials=DUCKDB_PATH),
                dataset_name="main",
            )
            info = pipeline.run(
                _records_gen(),
                table_name=table_name,
                write_disposition="append",
            )
            log.info("[%s] dlt loaded %s rows into '%s'. %s", repo, f"{len(records):,}", table_name, info)
            return
        except Exception as e:
            if attempt < max_retries - 1 and "lock" in str(e).lower():
                wait = 5 * (attempt + 1)
                log.warning("[%s] DuckDB locked, retrying in %ds (attempt %d/%d)...", repo, wait, attempt + 1, max_retries)
                time.sleep(wait)
            else:
                raise


def extract_and_load_repo(repo: str) -> None:
    """Extract and load a single repo. Used as an individual Airflow task."""
    log.info("Starting extraction for %s", repo)
    stargazers = get_stargazers(repo)
    log.info("[%s] Fetched %s total stargazers", repo, f"{len(stargazers):,}")
    load_with_dlt(stargazers, repo)


def run_extract_load() -> None:
    """Entry point for running all repos locally (outside Airflow)."""
    log.info("Extract-load start | DuckDB: %s", DUCKDB_PATH)
    for repo in REPOS:
        extract_and_load_repo(repo)
    log.info("Extract-load complete.")


if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1:
        # Called with a specific repo — used by Airflow BashOperator
        extract_and_load_repo(sys.argv[1])
    else:
        run_extract_load()
