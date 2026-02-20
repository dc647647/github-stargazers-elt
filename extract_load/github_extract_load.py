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

REPOS = [
    "dbt-labs/dbt-core",
    "apache/airflow",
    "dagster-io/dagster",
    "duckdb/duckdb",
    "dlt-hub/dlt",
]

GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")
DUCKDB_PATH = os.getenv(
    "DUCKDB_PATH",
    str(Path(__file__).parent.parent / "data" / "stargazers.duckdb"),
)

# GitHub caps stargazer results at 40,000 (400 pages × 100)
MAX_PAGES = 400
# Concurrent API requests — stays well within the 5,000 req/hr token limit
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
        print(
            "WARNING: No GITHUB_TOKEN set. "
            "Unauthenticated requests are limited to 60/hr."
        )
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
            print(f"  [{repo}] Rate limited on page {page}. Waiting {wait_secs:.0f}s...")
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
    Fetch all stargazers for a repo in parallel.
    - Fetches page 1 first to discover total page count from the Link header.
    - Fetches remaining pages concurrently with MAX_WORKERS threads.
    """
    session = _make_session()

    # Page 1 — needed to discover total pages via Link header
    _, first_records = _fetch_page(session, repo, 1)
    if not first_records:
        return []

    # Peek at the Link header to find the last page
    probe = session.get(
        f"https://api.github.com/repos/{repo}/stargazers",
        params={"per_page": 100, "page": 1},
    )
    last_page = min(_parse_last_page(probe.headers.get("Link", "")), MAX_PAGES)
    print(f"  [{repo}] {last_page} pages to fetch ({last_page * 100:,} records max)")

    if last_page == 1:
        return first_records

    # Fetch pages 2..last_page in parallel
    pages: dict[int, list[dict]] = {1: first_records}
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {
            executor.submit(_fetch_page, session, repo, p): p
            for p in range(2, last_page + 1)
        }
        for future in as_completed(futures):
            page_num, records = future.result()
            pages[page_num] = records
            if page_num % 50 == 0:
                print(f"  [{repo}] ...{page_num}/{last_page} pages done")

    # Reassemble in order
    return [record for p in sorted(pages) for record in pages[p]]


def load_to_duckdb(stargazers: list[dict], repo: str) -> None:
    """
    Load stargazer records into DuckDB.
    Full refresh per repo (delete + insert) keeps daily runs idempotent.
    """
    Path(DUCKDB_PATH).parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(DUCKDB_PATH)

    try:
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS raw_stargazers (
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

        con.execute("DELETE FROM raw_stargazers WHERE repo = ?", [repo])

        if stargazers:
            con.executemany(
                """
                INSERT INTO raw_stargazers
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

        count = con.execute(
            "SELECT COUNT(*) FROM raw_stargazers WHERE repo = ?", [repo]
        ).fetchone()[0]
        print(f"  [{repo}] Loaded {count:,} rows into DuckDB.")

    finally:
        con.close()


def run_extract_load() -> None:
    """
    Extract all repos in parallel, then load to DuckDB sequentially.
    (DuckDB doesn't support concurrent writes.)
    """
    print(f"EL start | DuckDB: {DUCKDB_PATH}\n")

    # Fetch all repos concurrently
    results: dict[str, list[dict]] = {}
    with ThreadPoolExecutor(max_workers=len(REPOS)) as executor:
        futures = {executor.submit(get_stargazers, repo): repo for repo in REPOS}
        for future in as_completed(futures):
            repo = futures[future]
            stargazers = future.result()
            results[repo] = stargazers
            print(f"[{repo}] Fetched {len(stargazers):,} total stargazers\n")

    # Load sequentially
    for repo in REPOS:
        load_to_duckdb(results[repo], repo)

    print("\nExtract-load complete.")


if __name__ == "__main__":
    run_extract_load()
