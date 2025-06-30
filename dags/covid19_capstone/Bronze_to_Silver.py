from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from sqlalchemy import create_engine
import pandas as pd

# Import your cleaning functions
from utils.stg_transformation import (
    clean_devices_data, clean_global_daily_data, clean_global_data, clean_global_table_data,
    clean_hosp_icu_data, clean_monthly_death_by_age_data, clean_observations_data,
    clean_encounters_data, clean_allergies_data, clean_careplans_data, clean_conditions_data,
    clean_covid_statewise_data, clean_imaging_studies_data, clean_immunizations_data,
    clean_medications_data, clean_organizations_data, clean_patients_data, clean_payers_data,
    clean_payer_transitions, clean_procedures_data, clean_providers, clean_supplies,
    clean_ods_vaccination_data
)

# --- Snowflake connection ---
sf_user = 'PRAGATI26'
sf_password = 'KhGJZf9nadNdcth'
sf_account = 'DCXDSBD-DC67829'
sf_warehouse = 'COMPUTE_WH'
sf_database = 'COVID19'
sf_role = 'ACCOUNTADMIN'

bronze_engine = create_engine(
    f'snowflake://{sf_user}:{sf_password}@{sf_account}/{sf_database}/BRONZE'
    f'?warehouse={sf_warehouse}&role={sf_role}', future=True
)
silver_engine = create_engine(
    f'snowflake://{sf_user}:{sf_password}@{sf_account}/{sf_database}/SILVER'
    f'?warehouse={sf_warehouse}&role={sf_role}', future=True
)

# Mapping table names to cleaning functions
tables = {
    "ODS_DEVICES": clean_devices_data,
    "ODS_GLOBAL_DAILY_DATA": clean_global_daily_data,
    "ODS_GLOBAL_DATA": clean_global_data,
    "ODS_GLOBAL_TABLE_DATA": clean_global_table_data,
    "ODS_HOSP_ICU_DATA": clean_hosp_icu_data,
    "ODS_MONTHLY_DEATH_BY_AGE": clean_monthly_death_by_age_data,
    "ODS_OBSERVATIONS": clean_observations_data,
    "ODS_ENCOUNTERS": clean_encounters_data,
    "ODS_ALLERGIES": clean_allergies_data,
    "ODS_CAREPLANS": clean_careplans_data,
    "ODS_CONDITIONS": clean_conditions_data,
    "ODS_COVID_STATE_HISTORY": clean_covid_statewise_data,
    "ODS_IMAGING_STUDIES": clean_imaging_studies_data,
    "ODS_IMMUNIZATIONS": clean_immunizations_data,
    "ODS_MEDICATIONS": clean_medications_data,
    "ODS_ORGANIZATIONS": clean_organizations_data,
    "ODS_PATIENTS": clean_patients_data,
    "ODS_PAYERS": clean_payers_data,
    "ODS_PAYER_TRANSITIONS": clean_payer_transitions,
    "ODS_PROCEDURES": clean_procedures_data,
    "ODS_PROVIDERS": clean_providers,
    "ODS_SUPPLIES": clean_supplies,
    "ODS_VACCINATION_DATA": clean_ods_vaccination_data
}

# Core function to truncate and load data


def clean_and_load_to_silver(table_name, cleaning_function):
    df = cleaning_function(bronze_engine)
    with silver_engine.begin() as conn:
        conn.execute(f'TRUNCATE TABLE {table_name}')
        df.to_sql(table_name, con=conn, index=False, if_exists='append')


# Define the DAG
with DAG(
    dag_id='clean_and_load_all_silver',
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=['silver', 'cleaning', 'truncate']
) as dag:

    for table_name, cleaning_function in tables.items():
        PythonOperator(
            task_id=f"clean_and_load_{table_name.lower()}",
            python_callable=clean_and_load_to_silver,
            op_args=[table_name, cleaning_function]
        )
