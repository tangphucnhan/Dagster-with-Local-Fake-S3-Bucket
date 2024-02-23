from dagster import ScheduleDefinition, DefaultScheduleStatus
from cafebills.jobs.etl_aws_s3 import job_etl_aws


daily_running = ScheduleDefinition(
    job=job_etl_aws,
    cron_schedule="0 17 * * *",
    execution_timezone="Asia/Singapore",
    default_status=DefaultScheduleStatus.RUNNING
)
