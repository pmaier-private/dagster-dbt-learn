# Learn project dbt + dagster

I want to understand how to use dbt and dagster in building data infrastructure like datalakes or lakehouses.

The usecase is ingesting transcribed voice call data from raw json objects stored in S3 to a postgres mart for aggregates of call lengths.

A voice call has a transcript and timestamp, id, event type, caller name and company name as meta data. 

The task here is to build the data infrastructure with dagster and dbt to ingest the raw data from a landing site in AWS, an S3 bucket, and provide views in postgres for further analysis. The latte are used inside a streamlit dashboard to display some metrics on the voice calls.

# Install & Run

## 1) Prerequisites

- Docker + Docker Compose
- Python 3.12+
- `uv` package manager
- A LocalStack auth token (the compose file uses `localstack/localstack-pro`)

## 2) Install dependencies

From the repository root:

```bash
uv sync
```

Install dependencies for the Dagster project too:

```bash
cd dagster_lab
uv sync
cd ..
```

## 3) Configure environment variables

Create or update `./.env` in the repository root:

```bash
DASHBOARD_DB_HOST=localhost
DASHBOARD_DB_PORT=5432
DASHBOARD_DB_NAME=dagster
DASHBOARD_DB_USER=postgres
DASHBOARD_DB_PASSWORD=postgres
DASHBOARD_DB_SCHEMA=dagster
DASHBOARD_DB_TABLE=fct_calls
LOCALSTACK_AUTH_TOKEN=<your-localstack-token>
```

Create or update `dagster_lab/.env`:

```bash
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=dagster
POSTGRES_USER=postgres
POSTGRES_PASSWORD=postgres

AWS_ACCESS_KEY_ID=test
AWS_SECRET_ACCESS_KEY=test
AWS_DEFAULT_REGION=eu-central-1
```

Create `~/.dbt/profiles.yml` (if missing) with a `dbt_lab` profile:

```yaml
dbt_lab:
	target: dev
	outputs:
		dev:
			type: postgres
			host: localhost
			port: 5432
			user: postgres
			password: postgres
			dbname: dagster
			schema: dagster
			threads: 4
```

## 4) Start infrastructure (Postgres + LocalStack)

```bash
docker compose up -d
docker compose ps
```

## 5) Upload raw sample data to LocalStack S3

From the repository root:

```bash
uv run bash init_raw.sh
```

This creates bucket `dagster-dbt-raw` and uploads `sample_data/sample_calls_25.csv`.

## 6) Run Dagster + dbt pipeline

Start Dagster UI from the `dagster_lab` directory:

```bash
cd dagster_lab
uv run dg dev
```

Open http://localhost:3000 and materialize:

- `dagster/raw_table` (loads S3 CSV into Postgres)
- the dbt assets (models/tests from `dbt_lab`)

Optional: run dbt directly from CLI:

```bash
cd dbt_lab
uv run dbt run --profiles-dir ~/.dbt
uv run dbt test --profiles-dir ~/.dbt
cd ..
```

## 7) Run the Streamlit dashboard

From the repository root:

```bash
uv run streamlit run src/dashboard/app.py
```

Then open the URL shown by Streamlit (usually http://localhost:8501).

## 8) Stop everything

```bash
docker compose down
```

# Learning Plan

## dbt

### excercises for top five dbt concepts: models, sources & project structure, dependencies and references, tests and QA, docs and linage

## dagster 

### understand assets, asset checks, schedules & jobs by building a simple two-asset DAG that copies a csv file and then stores the top N rows

* testing with some unit tests, but primarily with the dagster dev webUI

## dagster + dbt + raw data landing in localstack s3

### integrate dbt by defining a DbtProjectComponent in a defs.yaml pointing to the existing dbt project

### get the combined dbt + dagster DAG running successfully

Integrating the dbt assets worked almost out-of-the-box, just had to figure out some small things like providing the dbt profiles dir and some meta-data issues for one asset.

Executing the DAG failed at first, and it was a little difficult to see that actually two of the dbt data tests where failing. After I figured that out I soon could trace back the data issues in the mart views to a change / addition I made in the raw data. There I had added another call, but accidentally introduced an inconsistent call key that would lead to the same call being treated as two different, broken calls. After fixing that the DAG worked as expected.


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
