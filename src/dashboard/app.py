"""Streamlit dashboard for call session analytics.

Metrics:
- Call lengths per company
- Call lengths per caller
- Hang-ups per company
"""

from __future__ import annotations

import os
from typing import Any

import altair as alt
import pandas as pd
import psycopg2
import streamlit as st  # type: ignore[reportMissingImports]


st.set_page_config(page_title="Voice Calls Dashboard", layout="wide")


@st.cache_data(ttl=60)
def load_calls_data() -> pd.DataFrame:
    """Load call-session rows from the dbt mart model."""
    db_host = os.getenv("DASHBOARD_DB_HOST", "localhost")
    db_port = int(os.getenv("DASHBOARD_DB_PORT", "5432"))
    db_name = os.getenv("DASHBOARD_DB_NAME", "dagster")
    db_user = os.getenv("DASHBOARD_DB_USER", "postgres")
    db_password = os.getenv("DASHBOARD_DB_PASSWORD", "postgres")
    db_schema = os.getenv("DASHBOARD_DB_SCHEMA", "public")
    db_table = os.getenv("DASHBOARD_DB_TABLE", "fct_calls")

    query = (
        "select call_id, caller_name, caller_company, duration_seconds, is_hangup "
        f'from "{db_schema}"."{db_table}"'
    )

    with psycopg2.connect(
        host=db_host,
        port=db_port,
        dbname=db_name,
        user=db_user,
        password=db_password,
    ) as conn:
        # pandas accepts DB-API connections at runtime, but static typing is narrower.
        db_connection: Any = conn
        df = pd.read_sql_query(query, db_connection)

    if df.empty:
        return df

    df["duration_seconds"] = pd.to_numeric(df["duration_seconds"], errors="coerce").fillna(0)
    df["is_hangup"] = df["is_hangup"].astype(bool)
    df["caller_company"] = df["caller_company"].fillna("Unknown")
    df["caller_name"] = df["caller_name"].fillna("Unknown")

    return df


def aggregate_call_lengths_by_company(df: pd.DataFrame) -> pd.DataFrame:
    return (
        df.groupby("caller_company", as_index=False)
        .agg(
            total_calls=("call_id", "count"),
            avg_duration_seconds=("duration_seconds", "mean"),
            total_duration_seconds=("duration_seconds", "sum"),
        )
        .sort_values("total_duration_seconds", ascending=False)
    )


def aggregate_call_lengths_by_caller(df: pd.DataFrame) -> pd.DataFrame:
    return (
        df.groupby(["caller_company", "caller_name"], as_index=False)
        .agg(
            total_calls=("call_id", "count"),
            avg_duration_seconds=("duration_seconds", "mean"),
            total_duration_seconds=("duration_seconds", "sum"),
        )
        .sort_values("total_duration_seconds", ascending=False)
    )


def aggregate_hangups_by_company(df: pd.DataFrame) -> pd.DataFrame:
    return (
        df.groupby("caller_company", as_index=False)
        .agg(hangups=("is_hangup", "sum"), total_calls=("call_id", "count"))
        .assign(hangup_rate=lambda d: d["hangups"] / d["total_calls"])
        .sort_values("hangups", ascending=False)
    )


def aggregate_call_counts_by_company_stack(df: pd.DataFrame) -> pd.DataFrame:
    return (
        df.groupby("caller_company", as_index=False)
        .agg(call_count=("call_id", "count"))
        .sort_values("call_count", ascending=False)
        .assign(metric="All calls")
    )


def main() -> None:
    st.title("Voice Call Sessions Dashboard")
    st.caption("Powered by dbt mart model fct_calls")

    calls_df = pd.DataFrame()
    try:
        calls_df = load_calls_data()
    except Exception as exc:  # pragma: no cover
        st.error(f"Failed to load data from Postgres: {exc}")
        st.stop()

    if calls_df.empty:
        st.warning("No call-session data found in the configured table.")
        st.stop()

    companies = sorted(calls_df["caller_company"].dropna().unique().tolist())
    selected_companies = st.multiselect(
        "Filter by company",
        options=companies,
        default=companies,
    )

    filtered = calls_df[calls_df["caller_company"].isin(selected_companies)]

    total_calls = len(filtered)
    avg_duration = filtered["duration_seconds"].mean()
    hangup_count = int(filtered["is_hangup"].sum())

    c1, c2, c3 = st.columns(3)
    c1.metric("Calls", f"{total_calls:,}")
    c2.metric("Avg call length (sec)", f"{avg_duration:,.1f}")
    c3.metric("Hang-ups", f"{hangup_count:,}")

    by_company = aggregate_call_lengths_by_company(filtered)
    by_caller = aggregate_call_lengths_by_caller(filtered)
    hangups_by_company = aggregate_hangups_by_company(filtered)
    counts_stacked = aggregate_call_counts_by_company_stack(filtered)

    stack_chart = (
        alt.Chart(counts_stacked)
        .mark_bar()
        .encode(
            x=alt.X("metric:N", title=""),
            y=alt.Y("call_count:Q", title="Call count"),
            color=alt.Color("caller_company:N", title="Company"),
            tooltip=["caller_company", "call_count"],
        )
    )

    col_left, col_right = st.columns(2)
    with col_left:
        st.subheader("Call lengths per company")
        st.bar_chart(by_company.set_index("caller_company")["total_duration_seconds"])

    with col_right:
        st.subheader("Call counts per company (stacked)")
        st.altair_chart(stack_chart, use_container_width=True)

    st.dataframe(by_company, use_container_width=True)

    st.subheader("Call lengths per caller")
    st.dataframe(by_caller, use_container_width=True)

    st.subheader("Hang-ups per company")
    st.bar_chart(hangups_by_company.set_index("caller_company")["hangups"])
    st.dataframe(hangups_by_company, use_container_width=True)


if __name__ == "__main__":
    main()
