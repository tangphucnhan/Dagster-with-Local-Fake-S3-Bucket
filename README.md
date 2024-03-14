# cafebills

This is a Dagster project that put daily report files to AWS S3 then these files will be loaded to postgresql at a scheduled time.

It uses s3_fake_resource, S3FakeSession to imitate S3 environment, and we can check at local:

```
s3_session.put_object(
    Bucket=os.getenv("S3_BUCKET"),
    Key=f"{os.getenv('S3_PREFIX')}/{str_date}/{str_datetime}_{i}.csv",
    Body=bytes_data,
)
```
```
bytes_obj = s3_session.get_object(
    Bucket=os.getenv("S3_BUCKET"),
    Key=file["Key"],
)
```
