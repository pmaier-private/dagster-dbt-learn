import dagster as dg


@dg.schedule(cron_schedule="57 15 * * *", target="*")
def schedules(context: dg.ScheduleEvaluationContext) -> dg.RunRequest | dg.SkipReason:
    return dg.SkipReason(
        "Skipping. Change this to return a RunRequest to launch a run."
    )
