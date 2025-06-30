from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from covid19_capstone.ods.table_creation import create_bronze_ods_tables
from covid19_capstone.ods.load_ods import load_ods_data

default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 1, 1),
    "retries": 1,
}

with DAG(
    dag_id="s3_to_snowflake_ods_pipeline",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    description="End-to-end pipeline: upload CSV, convert to Parquet, create tables, and load data to ODS",
) as dag:
    
    task_create_tables = PythonOperator(
        task_id="create_bronze_ods_tables",
        python_callable=create_bronze_ods_tables,
    )

    task_load_data = PythonOperator(
        task_id="load_data_to_ods",
        python_callable=load_ods_data,
    )

    task_create_tables >> task_load_data
