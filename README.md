# Learn project dbt + dagster

I want to understand how to use dbt and dagster in building data infrastructure like datalakes or lakehouses.

The usecase is ingesting transcribed voice call data from raw json objects stored in S3 to a postgres mart for aggregates of call lengths.

A voice call has a transcript and timestamp, id, event type, caller name and company name as meta data. 


# Learning Plan

## dbt

### setup

### excercises for top five dbt concepts: models, sources & project structure, dependencies and references, tests and QA, docs and linage

## dagster 

TBD


# Streamlit dashboard

This project includes a Streamlit dashboard for call-session analytics:

- call lengths per company
- call lengths per caller
- hang-ups per company

The app is located at `src/dashboard/app.py`.

Run it with:

```bash
uv run streamlit run src/dashboard/app.py
```

Database connection values are read from environment variables (with defaults):

- `DASHBOARD_DB_HOST` (`localhost`)
- `DASHBOARD_DB_PORT` (`5432`)
- `DASHBOARD_DB_NAME` (`dagster`)
- `DASHBOARD_DB_USER` (`postgres`)
- `DASHBOARD_DB_PASSWORD` (`postgres`)
- `DASHBOARD_DB_SCHEMA` (`public`)
- `DASHBOARD_DB_TABLE` (`fct_calls`)


# TODO

- [] add indices
- [] raw data as seed
