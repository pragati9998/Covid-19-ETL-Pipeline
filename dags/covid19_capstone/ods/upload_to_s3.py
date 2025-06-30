from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import pandas as pd
import requests
import io
import zipfile

BUCKET_NAME = "covid19-raw-data-bucket"

WHO_DATA_URLS = {
    "global_daily": "https://srhdpeuwpubsa.blob.core.windows.net/whdh/COVID/WHO-COVID-19-global-daily-data.csv",
    "global": "https://srhdpeuwpubsa.blob.core.windows.net/whdh/COVID/WHO-COVID-19-global-data.csv",
    "table": "https://srhdpeuwpubsa.blob.core.windows.net/whdh/COVID/WHO-COVID-19-global-table-data.csv",
    "monthly_death_by_age": "https://srhdpeuwpubsa.blob.core.windows.net/whdh/COVID/WHO-COVID-19-global-monthly-death-by-age-data.csv",
    "hosp_icu": "https://srhdpeuwpubsa.blob.core.windows.net/whdh/COVID/WHO-COVID-19-global-hosp-icu-data.csv",
    "vaccination": "https://srhdpeuwpubsa.blob.core.windows.net/whdh/COVID/vaccination-data.csv",
    "all_states": "https://covidtracking.com/data/download/all-states-history.csv"
}

MITRE_ZIP_URL = "https://mitre.box.com/shared/static/9iglv8kbs1pfi7z8phjl9sbpjk08spze.zip"


def clean_column_names(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = [col.replace('\ufeff', '').strip() for col in df.columns]
    return df


def upload_who_parquet_to_s3():
    s3 = S3Hook(aws_conn_id="aws_default")

    for name, url in WHO_DATA_URLS.items():
        print(f"📥 Downloading WHO file: {name}...")
        response = requests.get(url)
        if response.status_code != 200:
            raise Exception(f"Failed to download {name}. Status: {response.status_code}")
        
        # Load CSV into DataFrame
        df = pd.read_csv(io.StringIO(response.text))
        df = clean_column_names(df)

        # Convert to Parquet
        buffer = io.BytesIO()
        df.to_parquet(buffer, engine='pyarrow', index=False)
        buffer.seek(0)

        # Upload to S3
        s3_key = f"covid/raw/all_data/{name}.parquet"
        s3.load_bytes(buffer.read(), key=s3_key, bucket_name=BUCKET_NAME, replace=True)
        print(f"✅ Uploaded WHO file as Parquet: {s3_key}")


def upload_mitre_zip_parquet_to_s3():
    s3 = S3Hook(aws_conn_id='aws_default')

    print("📥 Downloading MITRE ZIP...")
    response = requests.get(MITRE_ZIP_URL)
    if response.status_code != 200:
        raise Exception(f"Failed to download MITRE zip. Status: {response.status_code}")

    with zipfile.ZipFile(io.BytesIO(response.content)) as zip_file:
        for file_info in zip_file.infolist():
            if file_info.filename.endswith(".csv"):
                print(f"📄 Extracting: {file_info.filename}")
                with zip_file.open(file_info) as f:
                    df = pd.read_csv(f)
                    df = clean_column_names(df)

                    buffer = io.BytesIO()
                    df.to_parquet(buffer, engine='pyarrow', index=False)
                    buffer.seek(0)

                    base_name = file_info.filename.split("/")[-1].replace('.csv', '.parquet')
                    s3_key = f"covid/raw/all_data/{base_name}"

                    s3.load_bytes(buffer.read(), key=s3_key, bucket_name=BUCKET_NAME, replace=True)
                    print(f"✅ Uploaded MITRE file as Parquet: {s3_key}")
