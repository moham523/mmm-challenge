"""Weekly schedule for the mart pipeline, plus a concurrency limit.

The concurrency tag matters because SQLite locks the whole database file on
writes -- two overlapping runs (e.g. a manual click while the schedule fires)
would fight over the file and one would fail with "database is locked"
instead of just waiting its turn.
"""

from dagster import AssetSelection, ScheduleDefinition, define_asset_job

mmm_mart_job = define_asset_job(
    name="mmm_mart_job",
    selection=AssetSelection.all(),
    tags={"dagster/concurrency_key": "mmm_sqlite"},
)

# Runs every Monday at 6am, once the prior week's spend/transactions/CPI data
# should all be available.
mmm_mart_weekly_schedule = ScheduleDefinition(
    name="mmm_mart_weekly_schedule",
    job=mmm_mart_job,
    cron_schedule="0 6 * * 1",
)
