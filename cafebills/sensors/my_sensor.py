from dagster import RunRequest, sensor
from cafebills.jobs.etl_aws_s3 import job_etl_aws


@sensor(job=job_etl_aws)
def my_sensor(context):
    should_run = True
    if should_run:
        yield RunRequest(run_key=None, run_config={})
