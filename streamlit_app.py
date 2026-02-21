import os
from pathlib import Path

import duckdb
import plotly.express as px
import streamlit as st

DUCKDB_PATH = os.getenv(
    "DUCKDB_PATH",
    str(Path(__file__).parent / "data" / "stargazers.duckdb"),
)

st.set_page_config(page_title="GitHub Stargazers Dashboard", layout="wide")
st.title("GitHub Stargazers Dashboard")
st.caption(f"Data source: `{DUCKDB_PATH}`")

con = duckdb.connect(DUCKDB_PATH, read_only=True)


# ── Stars per repo ──────────────────────────────────────────────────────────
st.subheader("Total Stars per Repo")
stars_per_repo = con.execute("SELECT * FROM agg_stars_per_repo").df()
fig = px.bar(
    stars_per_repo,
    x="repo",
    y="total_stars",
    text="total_stars",
    color="repo",
)
fig.update_traces(textposition="outside")
fig.update_layout(showlegend=False, xaxis_title="", yaxis_title="Total Stars")
st.plotly_chart(fig, use_container_width=True)


# ── Stars over time ─────────────────────────────────────────────────────────
st.subheader("Cumulative Stars Over Time")
stars_over_time = con.execute("SELECT * FROM agg_stars_over_time").df()
fig2 = px.line(
    stars_over_time,
    x="starred_date",
    y="cumulative_stars",
    color="repo",
    labels={"starred_date": "Date", "cumulative_stars": "Cumulative Stars", "repo": "Repo"},
)
st.plotly_chart(fig2, use_container_width=True)


# ── Stargazer overlap ───────────────────────────────────────────────────────
st.subheader("Stargazer Overlap — How Many Repos Did Users Star?")
overlap = con.execute("SELECT * FROM agg_stargazer_overlap").df()
overlap["repos_starred_count"] = overlap["repos_starred_count"].astype(str)
fig3 = px.bar(
    overlap,
    x="repos_starred_count",
    y="user_count",
    text="user_count",
    labels={"repos_starred_count": "Number of Repos Starred", "user_count": "Number of Users"},
)
fig3.update_traces(textposition="outside")
fig3.update_layout(xaxis_title="Number of Repos Starred", yaxis_title="Number of Users")
st.plotly_chart(fig3, use_container_width=True)


# ── Top stargazers ──────────────────────────────────────────────────────────
st.subheader("Top Stargazers (Starred the Most Repos)")
top_users = con.execute("""
    SELECT
        user_login,
        repos_starred_count,
        repos_starred,
        first_starred_at,
        last_starred_at
    FROM stargazer_summary
    WHERE repos_starred_count > 1
    ORDER BY repos_starred_count DESC
    LIMIT 50
""").df()
st.dataframe(top_users, use_container_width=True)

con.close()
