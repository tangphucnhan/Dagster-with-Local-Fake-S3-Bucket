from dagster import job
from dagster_aws.s3 import s3_resource
from cafebills.ops.ops_with_aws import fetch_s3_objects, load_to_postgres


@job(resource_defs={'s3': s3_resource})
def job_etl_aws():
    df = fetch_s3_objects()
    load_to_postgres(df)
