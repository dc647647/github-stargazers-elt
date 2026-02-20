import os
import time
import duckdb
import requests
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


def get_stargazers(repo: str) -> list[dict]:
    """Fetch all stargazers for a repo from the GitHub API (paginated)."""
    headers = {
        "Accept": "application/vnd.github.star+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }
    if GITHUB_TOKEN:
        headers["Authorization"] = f"Bearer {GITHUB_TOKEN}"
    else:
        print(
            "WARNING: No GITHUB_TOKEN set. "
            "Unauthenticated requests are limited to 60/hr. "
            "Set GITHUB_TOKEN in your .env file."
        )

    stargazers: list[dict] = []
    page = 1

    while True:
        url = f"https://api.github.com/repos/{repo}/stargazers"
        response = requests.get(
            url,
            headers=headers,
            params={"per_page": 100, "page": page},
        )

        # Handle rate limiting
        if response.status_code in (403, 429):
            reset_ts = int(response.headers.get("X-RateLimit-Reset", time.time() + 60))
            wait_secs = max(reset_ts - time.time(), 0) + 5
            print(f"  Rate limited. Waiting {wait_secs:.0f}s...")
            time.sleep(wait_secs)
            continue

        response.raise_for_status()
        data = response.json()

        if not data:
            break

        extracted_at = datetime.now(timezone.utc).isoformat()
        for item in data:
            stargazers.append(
                {
                    "user_login": item["user"]["login"],
                    "user_id": item["user"]["id"],
                    "repo": repo,
                    "starred_at": item.get("starred_at"),
                    "avatar_url": item["user"]["avatar_url"],
                    "html_url": item["user"]["html_url"],
                    "extracted_at": extracted_at,
                }
            )

        print(f"  Page {page}: fetched {len(data)} records (total: {len(stargazers)})")
        page += 1

        # Proactively back off when remaining quota is low
        remaining = int(response.headers.get("X-RateLimit-Remaining", 5000))
        if remaining < 10:
            reset_ts = int(response.headers.get("X-RateLimit-Reset", time.time() + 60))
            wait_secs = max(reset_ts - time.time(), 0) + 5
            print(f"  Rate limit low ({remaining} left). Waiting {wait_secs:.0f}s...")
            time.sleep(wait_secs)

    return stargazers


def load_to_duckdb(stargazers: list[dict], repo: str) -> None:
    """
    Load stargazer records into DuckDB.
    Uses a full refresh per repo (delete + insert) to stay idempotent
    across daily runs — handles both new stars and removed stars correctly.
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

        # Full refresh for this repo so daily re-runs are safe
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
        print(f"  Loaded {count} rows for '{repo}' into DuckDB.")

    finally:
        con.close()


def run_extract_load() -> None:
    """Entry point: extract stargazers for all repos and load to DuckDB."""
    print(f"EL start | DuckDB path: {DUCKDB_PATH}\n")
    for repo in REPOS:
        print(f"[{repo}] Extracting...")
        stargazers = get_stargazers(repo)
        print(f"[{repo}] Total fetched: {len(stargazers)}")
        load_to_duckdb(stargazers, repo)
        print()
    print("Extract-load complete.")


if __name__ == "__main__":
    run_extract_load()
