import json

import dagster as dg
from .assets import head_it, copy_it
import os


default_etl_job = dg.define_asset_job(
    "default_etl_job", selection=[head_it, copy_it]
)

upload_job = dg.define_asset_job(
    "upload_job",
    description="Uploads / copies data upon file changes",
    selection=[copy_it],
)


@dg.sensor(
    job=upload_job,
    minimum_interval_seconds=15,
    default_status=dg.DefaultSensorStatus.RUNNING,
    description=(
        "Sensor that watches for changes in the sample data file "
        "and triggers the upload job"
    ),
)
def csv_sensor(
    context: dg.SensorEvaluationContext,
) -> dg.SensorResult:

    file_name = "sample_calls_25.csv"
    file_path = f"../sample_data/{file_name}"
    recently_modified = os.path.getmtime(file_path)

    last_modified = (
        json.loads(context.cursor) if context.cursor else recently_modified
    )

    runs = []
    if last_modified != recently_modified:
        runs.append(
            dg.RunRequest(
                run_key=f"{file_name}_{recently_modified}", run_config={}
            )
        )

    return dg.SensorResult(
        run_requests=runs, cursor=json.dumps(recently_modified)
    )


@dg.schedule(
    job=default_etl_job,
    cron_schedule="58 10 * * *",
    execution_timezone="CET",
    default_status=dg.DefaultScheduleStatus.RUNNING,
    description="Schedule for the standard daily ETL",
)
def schedules(
    context: dg.ScheduleEvaluationContext,
) -> dg.RunRequest | dg.SkipReason:
    return dg.RunRequest(
        "schedules_run_" + context.scheduled_execution_time.isoformat()
    )
