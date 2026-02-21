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

con = duckdb.connect(DUCKDB_PATH, read_only=True)

last_updated = con.execute("SELECT MAX(extracted_at) FROM dim_stargazers").fetchone()[0]
last_updated_str = last_updated.strftime("%B %d, %Y %H:%M UTC") if last_updated else "unknown"

st.title("GitHub Stargazers Dashboard")
st.caption(f"Data last updated: {last_updated_str}")

tab_repo, tab_user = st.tabs(["Repo Analytics", "User Analytics"])


# ── TAB 1: Repo Analytics ────────────────────────────────────────────────────
with tab_repo:

    # Stars per repo
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

    # Cumulative stars over time
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

    # Stargazer overlap
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


# ── TAB 2: User Analytics ────────────────────────────────────────────────────
with tab_user:

    user_activity = con.execute("SELECT * FROM agg_user_activity").df()

    # KPIs
    total_users    = len(user_activity)
    multi_repo     = (user_activity["repos_starred_count"] > 1).sum()
    all_five       = (user_activity["repos_starred_count"] == 5).sum()

    k1, k2, k3 = st.columns(3)
    k1.metric("Total Unique Users", f"{total_users:,}")
    k2.metric("Starred 2+ Repos", f"{multi_repo:,}", help="Users who starred more than one tracked repo")
    k3.metric("Starred All 5 Repos", f"{all_five:,}", help="Users who starred every tracked repo")

    st.divider()

    # Avg days between stars histogram
    st.subheader("Avg Days Between Stars (users with 2+ stars)")
    multi = user_activity[
        (user_activity["repos_starred_count"] > 1) &
        user_activity["avg_days_between_stars"].notna()
    ].copy()

    fig4 = px.histogram(
        multi[multi["avg_days_between_stars"] < 730],  # cap at 2 years for readability
        x="avg_days_between_stars",
        nbins=60,
        labels={"avg_days_between_stars": "Avg Days Between Stars"},
    )
    fig4.update_layout(
        xaxis_title="Avg Days Between Stars",
        yaxis_title="Number of Users",
        bargap=0.05,
    )
    st.plotly_chart(fig4, use_container_width=True)
    st.caption(
        f"Median: {multi['avg_days_between_stars'].median():.0f} days · "
        f"Users who binge-starred same day (≤1 day): "
        f"{(multi['avg_days_between_stars'] <= 1).sum():,}"
    )

    st.divider()

    # Top users leaderboard
    st.subheader("Top Users — Most Repos Starred")
    top_users = user_activity[user_activity["repos_starred_count"] > 1][
        ["user_login", "repos_starred_count", "repos_starred", "first_starred_at", "last_starred_at", "avg_days_between_stars"]
    ].head(50).copy()
    top_users["first_starred_at"] = top_users["first_starred_at"].dt.date
    top_users["last_starred_at"]  = top_users["last_starred_at"].dt.date
    top_users["avg_days_between_stars"] = top_users["avg_days_between_stars"].round(1)
    st.dataframe(top_users, use_container_width=True, hide_index=True)

    st.divider()

    # Per-user star timeline drilldown
    st.subheader("User Star Timeline")

    multi_repo_users = (
        user_activity[user_activity["repos_starred_count"] > 1]["user_login"]
        .sort_values()
        .tolist()
    )
    selected_user = st.selectbox(
        "Select a user (only users who starred 2+ repos shown)",
        options=multi_repo_users,
        index=multi_repo_users.index("provpup") if "provpup" in multi_repo_users else 0,
    )

    if selected_user:
        user_stars = con.execute(
            "SELECT repo, starred_at FROM dim_stargazers WHERE user_login = ? ORDER BY starred_at",
            [selected_user],
        ).df()

        fig5 = px.scatter(
            user_stars,
            x="starred_at",
            y="repo",
            color="repo",
            symbol="repo",
            labels={"starred_at": "Starred At", "repo": "Repo"},
            title=f"Star timeline for {selected_user}",
        )
        fig5.update_traces(marker_size=12)
        fig5.update_layout(showlegend=False, yaxis_title="")
        st.plotly_chart(fig5, use_container_width=True)

        user_row = user_activity[user_activity["user_login"] == selected_user].iloc[0]
        c1, c2, c3 = st.columns(3)
        c1.metric("Repos Starred", int(user_row["repos_starred_count"]))
        c2.metric("First Star", str(user_row["first_starred_at"].date()))
        c3.metric("Avg Days Between Stars",
                  f"{user_row['avg_days_between_stars']:.1f}" if user_row["avg_days_between_stars"] else "N/A")


con.close()
