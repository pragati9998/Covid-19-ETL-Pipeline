import boto3
import pandas as pd
import io
import os
from sqlalchemy import create_engine, text

# ----- CONFIGURATION -----
BUCKET = "covid19-raw-data-bucket"
PARQUET_PREFIX = "covid/raw/parquet/"
CSV_PREFIX = "covid/raw/csv/"
SCHEMA = "BRONZE"
STAGE = "@my_s3_stage"

# ----- Snowflake Engine -----
def get_snowflake_engine(schema=SCHEMA):
    user = 'ASHWINI'
    password = 'VE89b2CYDC86Qew'
    account = 'LYEYLDE-OE27924'
    database = 'COVID19'
    warehouse = 'COMPUTE_WH'

    return create_engine(
        f'snowflake://{user}:{password}@{account}/{database}/{schema}?warehouse={warehouse}'
    )

# ----- TASK 2: Process Parquet (Infer Schema) -----
def infer_parquet_schema():
    s3 = boto3.client("s3")
    response = s3.list_objects_v2(Bucket=BUCKET, Prefix=PARQUET_PREFIX)

    jobs = []

    for obj in response.get("Contents", []):
        key = obj["Key"]
        if not key.endswith(".parquet"):
            continue

        obj_data = s3.get_object(Bucket=BUCKET, Key=key)
        df = pd.read_parquet(io.BytesIO(obj_data["Body"].read()))

        table_name = "ODS_" + key.split("/")[-1].replace(".parquet", "").upper()
        columns = [(col, str(df[col].dtype)) for col in df.columns]

        jobs.append({
            "table": table_name,
            "s3_path": f"s3://{BUCKET}/{key}",
            "columns": columns
        })

    return jobs

# ----- Helper: Pandas to Snowflake types -----
def map_dtype_to_snowflake(dtype_str):
    if 'int' in dtype_str:
        return "NUMBER"
    elif 'float' in dtype_str:
        return "FLOAT"
    elif 'bool' in dtype_str:
        return "BOOLEAN"
    elif 'datetime' in dtype_str:
        return "TIMESTAMP_NTZ"
    else:
        return "VARCHAR"

# ----- TASK 3: Create ODS Tables -----
def create_ods_tables():
    jobs = infer_parquet_schema()
    engine = get_snowflake_engine()

    with engine.connect() as conn:
        for job in jobs:
            col_defs = ",\n".join(
                [f'"{col}" {map_dtype_to_snowflake(dtype)}' for col, dtype in job["columns"]]
            )
            ddl = f"""
            CREATE OR REPLACE TABLE {SCHEMA}.{job['table']} (
                {col_defs},
                business_date DATE DEFAULT CURRENT_DATE
            )
            """
            conn.execute(text(ddl))
            print(f"✅ Created table: {job['table']}")

# ----- TASK 4: Truncate ODS Tables -----
def truncate_ods_tables():
    jobs = infer_parquet_schema()
    engine = get_snowflake_engine()

    with engine.connect() as conn:
        for job in jobs:
            conn.execute(text(f"TRUNCATE TABLE {SCHEMA}.{job['table']}"))
            print(f"🧹 Truncated: {job['table']}")

# ----- TASK 5: Load Data from S3 to Snowflake -----
def load_ods_data():
    jobs = infer_parquet_schema()
    engine = get_snowflake_engine()

    with engine.connect() as conn:
        for job in jobs:
            filename = job['s3_path'].split("/")[-1]
            copy_sql = f"""
            COPY INTO {SCHEMA}.{job['table']}
            FROM {STAGE}/{filename}
            FILE_FORMAT = (TYPE = PARQUET)
            """
            conn.execute(text(copy_sql))
            print(f"📥 Loaded data into: {job['table']}")
