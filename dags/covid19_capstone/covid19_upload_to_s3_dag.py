from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

from covid19_capstone.ods.upload_to_s3 import upload_mitre_zip_parquet_to_s3, upload_who_parquet_to_s3

default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 1, 1),
    "retries": 1,
}

with DAG(
    dag_id="upload_to_s3_pipeline",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    description="Upload WHO and MITRE data to S3"
) as dag:
    task_upload_who_parquet = PythonOperator(
        task_id="upload_who_csvs_to_s3",
        python_callable=upload_who_parquet_to_s3,
    )
    task_upload_fhir_parquet = PythonOperator(
        task_id="upload_mitre_zip_csvs_to_s3",
        python_callable=upload_mitre_zip_parquet_to_s3,
    )

    task_upload_who_parquet >> task_upload_fhir_parquet 
