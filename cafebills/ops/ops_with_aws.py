import json
import os
import ast
import pandas as pd
from dagster import op, Out, Output, OpExecutionContext
from sqlalchemy import create_engine
from dagster_aws.s3 import s3_fake_resource, S3FakeSession
import random as rd
import numpy as np
from dotenv import load_dotenv
from datetime import datetime as dt


load_dotenv()


def generate_s3_samples() -> S3FakeSession:
    s3_session: S3FakeSession = s3_fake_resource.create_s3_fake_resource()
    str_date = dt.today().strftime("%Y%m%d")
    str_datetime = dt.today().strftime("%Y%m%d_%H%M%S")
    os.makedirs(f"samples/{str_date}", exist_ok=True)
    items = ["Beer", "Cider", "Cocktails", "Hard soda", "Wine", "Barley", "Mixed drinks", "Coffee", "Hot chocolate", "Lemon tea"]
    count = len(items)
    cols = ["StoreId", "Item", "Quantity", "Price(USD)", "StaffId", "Datetime"]
    for i in range(1, 11):
        df_data = list()
        for j in range(100):
            df_data.append([
                i,
                items[rd.randint(0, count - 1)],
                rd.randint(1, 5),
                np.format_float_positional(np.random.rand(1) * 10, precision=2),
                rd.randint(1000, 5000),
                str_datetime,
            ])
        # Uncomment to write data to folder 'samples'
        # df = pd.DataFrame(df_data, index=None, columns=cols)
        # df.to_csv(f"samples/{str_date}/{str_datetime}_{i}.csv", index=False)

        json_str = json.JSONEncoder().encode(df_data)
        bytes_data = np.array(json_str).tobytes()
        s3_session.put_object(
            Bucket=os.getenv("S3_BUCKET"),
            Key=f"{os.getenv('S3_PREFIX')}/{str_date}/{str_datetime}_{i}.csv",
            Body=bytes_data,
        )
    return s3_session


@op(required_resource_keys={'s3'}, out={"df": Out(is_required=True)})
def fetch_s3_objects(context: OpExecutionContext):
    s3_session: S3FakeSession = generate_s3_samples()
    files = s3_session.list_objects_v2(
        Bucket=os.getenv("S3_BUCKET"),
        Prefix=os.getenv('S3_PREFIX'),
    )
    cols = ["StoreId", "Item", "Quantity", "Price(USD)", "StaffId", "Datetime"]
    list_data = list()
    for file in files["Contents"]:
        context.log.info(file["Key"])
        bytes_obj = s3_session.get_object(
            Bucket=os.getenv("S3_BUCKET"),
            Key=file["Key"],
        )
        data = bytes_obj["Body"].read().decode()
        # Remove null bytes
        data = data.replace("\0", "")
        data = ast.literal_eval(data)
        list_data.extend(data)
        s3_session.delete_object(
            Bucket=os.getenv("S3_BUCKET"),
            Key=file["Key"],
        )
    df = pd.DataFrame(list_data, index=None, columns=cols)
    yield Output(df, "df")


@op
def load_to_postgres(context: OpExecutionContext, df: pd.DataFrame):
    try:
        db_user = os.getenv("DB_USER")
        db_pwd = os.getenv("DB_PWD")
        db_host = os.getenv("DB_HOST")
        db_port = os.getenv("DB_PORT")
        db_name = os.getenv("DB_NAME")
        engine = create_engine(f"postgresql://{db_user}:{db_pwd}@{db_host}:{db_port}/{db_name}")
        count = df.to_sql(name='daily_report', con=engine, index=False, if_exists='append')
        context.log.info(f"!!!Inserted {count} rows of data")
    except Exception as e:
        context.log.info("Load data error:")
        context.log.info(e)
