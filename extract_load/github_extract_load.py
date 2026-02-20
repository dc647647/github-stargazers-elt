import logging
import os
import re
import time
import duckdb
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path

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

GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")
DUCKDB_PATH = os.getenv(
    "DUCKDB_PATH",
    str(Path(__file__).parent.parent / "data" / "stargazers.duckdb"),
)

# GitHub caps stargazer results at 40,000 (400 pages × 100)
MAX_PAGES = 400
# Concurrent API requests per task
MAX_WORKERS = 10


def _make_session() -> requests.Session:
    session = requests.Session()
    session.headers.update(
        {
            "Accept": "application/vnd.github.star+json",
            "X-GitHub-Api-Version": "2022-11-28",
        }
    )
    if GITHUB_TOKEN:
        session.headers["Authorization"] = f"Bearer {GITHUB_TOKEN}"
    else:
        log.warning("No GITHUB_TOKEN set. Unauthenticated requests are limited to 60/hr.")
    return session


def _parse_last_page(link_header: str) -> int:
    """Extract the last page number from a GitHub Link response header."""
    match = re.search(r'[?&]page=(\d+)>; rel="last"', link_header)
    return int(match.group(1)) if match else 1


def _fetch_page(session: requests.Session, repo: str, page: int) -> tuple[int, list[dict]]:
    """Fetch a single page of stargazers. Retries automatically on rate limits."""
    url = f"https://api.github.com/repos/{repo}/stargazers"
    while True:
        response = session.get(url, params={"per_page": 100, "page": page})

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
        extracted_at = datetime.now(timezone.utc).isoformat()

        records = [
            {
                "user_login": item["user"]["login"],
                "user_id": item["user"]["id"],
                "repo": repo,
                "starred_at": item.get("starred_at"),
                "avatar_url": item["user"]["avatar_url"],
                "html_url": item["user"]["html_url"],
                "extracted_at": extracted_at,
            }
            for item in data
        ]
        return page, records


def get_stargazers(repo: str) -> list[dict]:
    """
    Fetch all stargazers for a repo.
    - Fetches page 1 to discover total pages from the Link header.
    - Fetches remaining pages concurrently with MAX_WORKERS threads.
    Safe because each Airflow task is an isolated process and DuckDB
    is not touched until after all fetching is complete.
    """
    session = _make_session()

    _, first_records = _fetch_page(session, repo, 1)
    if not first_records:
        return []

    probe = session.get(
        f"https://api.github.com/repos/{repo}/stargazers",
        params={"per_page": 100, "page": 1},
    )
    last_page = min(_parse_last_page(probe.headers.get("Link", "")), MAX_PAGES)
    log.info("[%s] %d pages to fetch (~%s records)", repo, last_page, f"{last_page * 100:,}")

    if last_page == 1:
        return list(first_records)

    pages: dict[int, list[dict]] = {1: list(first_records)}
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {
            executor.submit(_fetch_page, session, repo, p): p
            for p in range(2, last_page + 1)
        }
        for future in as_completed(futures):
            page_num, records = future.result()
            pages[page_num] = records
            if page_num % 50 == 0:
                log.info("[%s] ...%d/%d pages done", repo, page_num, last_page)

    return [record for p in sorted(pages) for record in pages[p]]


def load_to_duckdb(stargazers: list[dict], repo: str) -> None:
    """
    Load stargazer records into a repo-specific DuckDB table.
    Each repo has its own table so parallel tasks never contend for locks.
    Full refresh (drop + recreate) keeps daily runs idempotent.
    """
    table = REPO_TABLE_MAP[repo]
    Path(DUCKDB_PATH).parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(DUCKDB_PATH)

    try:
        con.execute(f"DROP TABLE IF EXISTS {table}")
        con.execute(
            f"""
            CREATE TABLE {table} (
                user_login   VARCHAR,
                user_id      BIGINT,
                repo         VARCHAR,
                starred_at   TIMESTAMPTZ,
                avatar_url   VARCHAR,
                html_url     VARCHAR,
                extracted_at TIMESTAMPTZ
            )
            """
        )

        if stargazers:
            con.executemany(
                f"""
                INSERT INTO {table}
                    (user_login, user_id, repo, starred_at, avatar_url, html_url, extracted_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        s["user_login"],
                        s["user_id"],
                        s["repo"],
                        s["starred_at"],
                        s["avatar_url"],
                        s["html_url"],
                        s["extracted_at"],
                    )
                    for s in stargazers
                ],
            )

        count = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        log.info("[%s] Loaded %s rows into DuckDB table '%s'.", repo, f"{count:,}", table)

    finally:
        con.close()


def extract_and_load_repo(repo: str) -> None:
    """Extract and load a single repo. Used as an individual Airflow task."""
    log.info("Starting extraction for %s", repo)
    stargazers = get_stargazers(repo)
    log.info("[%s] Fetched %s total stargazers", repo, f"{len(stargazers):,}")
    load_to_duckdb(stargazers, repo)


def run_extract_load() -> None:
    """Entry point for running all repos locally (outside Airflow)."""
    log.info("Extract-load start | DuckDB: %s", DUCKDB_PATH)
    for repo in REPOS:
        extract_and_load_repo(repo)
    log.info("Extract-load complete.")


if __name__ == "__main__":
    run_extract_load()
