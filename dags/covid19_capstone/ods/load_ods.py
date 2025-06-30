from covid19_capstone.utils.snowflake_configuration import get_snowflake_engine
from sqlalchemy import text

def load_ods_data():
    engine = get_snowflake_engine(schema='BRONZE')

    reserved_keywords = {
        "DATE", "START", "STOP"
        # Add more Snowflake reserved keywords here as needed
    }

    def quote_if_reserved(name):
        if name.upper() in reserved_keywords:
            return f'"{name}"'
        else:
            return name

    # List of COPY INTO jobs
    copy_jobs = [
        {
            "table": "ODS_GLOBAL_DAILY_DATA",
            "s3_path": "s3://covid19-raw-data-bucket/covid/raw/all_data/global_daily.parquet",
            "columns": ["Date_reported", "Country_code", "Country", "WHO_region", "New_cases", "Cumulative_cases", "New_deaths", "Cumulative_deaths"]
        },
        {
            "table": "ODS_GLOBAL_DATA",
            "s3_path": "s3://covid19-raw-data-bucket/covid/raw/all_data/global.parquet",
            "columns": ["Date_reported", "Country_code", "Country", "WHO_region", "New_cases", "Cumulative_cases", "New_deaths", "Cumulative_deaths"]
        },
        {
            "table": "ODS_GLOBAL_TABLE_DATA",
            "s3_path": "s3://covid19-raw-data-bucket/covid/raw/all_data/table.parquet",
            "columns": ["Name", "WHO_Region", "Cases - cumulative total", "Cases - cumulative total per 100000 population", "Cases - newly reported in last 7 days", "Cases - newly reported in last 7 days per 100000 population", "Cases - newly reported in last 24 hours", "Deaths - cumulative total", "Deaths - cumulative total per 100000 population", "Deaths - newly reported in last 7 days", "Deaths - newly reported in last 7 days per 100000 population", "Deaths - newly reported in last 24 hours"]
        },
        {
            "table": "ODS_MONTHLY_DEATH_BY_AGE",
            "s3_path": "s3://covid19-raw-data-bucket/covid/raw/all_data/monthly_death_by_age.parquet",
            "columns": ["Country", "Country_code", "Who_region", "Wb_income", "Year", "Month", "Agegroup", "Deaths"]
        },
        {
            "table": "ODS_HOSP_ICU_DATA",
            "s3_path": "s3://covid19-raw-data-bucket/covid/raw/all_data/hosp_icu.parquet",
            "columns": ["Date_reported", "Country_code", "Country", "WHO_region", "Covid_new_hospitalizations_last_7days", "Covid_new_icu_admissions_last_7days", "Covid_new_hospitalizations_last_28days", "Covid_new_icu_admissions_last_28days"]
        },
        {
            "table": "ODS_VACCINATION_DATA",
            "s3_path": "s3://covid19-raw-data-bucket/covid/raw/all_data/vaccination.parquet",
            "columns": ["COUNTRY", "ISO3", "WHO_REGION", "DATA_SOURCE", "DATE_UPDATED", "TOTAL_VACCINATIONS", "PERSONS_VACCINATED_1PLUS_DOSE", "TOTAL_VACCINATIONS_PER100", "PERSONS_VACCINATED_1PLUS_DOSE_PER100", "PERSONS_LAST_DOSE", "PERSONS_LAST_DOSE_PER100", "VACCINES_USED", "FIRST_VACCINE_DATE", "NUMBER_VACCINES_TYPES_USED", "PERSONS_BOOSTER_ADD_DOSE", "PERSONS_BOOSTER_ADD_DOSE_PER100"]
        },
        {
            "table": "ODS_COVID_STATE_HISTORY",
            "s3_path": "s3://covid19-raw-data-bucket/covid/raw/all_data/all-states-history.parquet",
            "columns": ["DATE", "STATE", "DEATH", "DEATH_CONFIRMED", "DEATH_INCREASE", "DEATH_PROBABLE", "HOSPITALIZED", "HOSPITALIZED_CUMULATIVE", "HOSPITALIZED_CURRENTLY", "HOSPITALIZED_INCREASE", "IN_ICU_CUMULATIVE", "IN_ICU_CURRENTLY", "NEGATIVE", "NEGATIVE_INCREASE", "NEGATIVE_TESTS_ANTIBODY", "NEGATIVE_TESTS_PEOPLE_ANTIBODY", "NEGATIVE_TESTS_VIRAL", "ON_VENTILATOR_CUMULATIVE", "ON_VENTILATOR_CURRENTLY", "POSITIVE", "POSITIVE_CASES_VIRAL", "POSITIVE_INCREASE", "POSITIVE_SCORE", "POSITIVE_TESTS_ANTIBODY", "POSITIVE_TESTS_ANTIGEN", "POSITIVE_TESTS_PEOPLE_ANTIBODY", "POSITIVE_TESTS_PEOPLE_ANTIGEN", "POSITIVE_TESTS_VIRAL", "RECOVERED", "TOTAL_TEST_ENCOUNTERS_VIRAL", "TOTAL_TEST_ENCOUNTERS_VIRAL_INCREASE", "TOTAL_TEST_RESULTS", "TOTAL_TEST_RESULTS_INCREASE", "TOTAL_TESTS_ANTIBODY", "TOTAL_TESTS_ANTIGEN", "TOTAL_TESTS_PEOPLE_ANTIBODY", "TOTAL_TESTS_PEOPLE_ANTIGEN", "TOTAL_TESTS_PEOPLE_VIRAL", "TOTAL_TESTS_PEOPLE_VIRAL_INCREASE", "TOTAL_TESTS_VIRAL", "TOTAL_TESTS_VIRAL_INCREASE"]
        },
        {
            "table": "ODS_ENCOUNTERS",
            "s3_path": "s3://covid19-raw-data-bucket/covid/raw/all_data/encounters.parquet",
            "columns": ["ID", "START", "STOP", "PATIENT", "ORGANIZATION", "PROVIDER", "PAYER", "ENCOUNTERCLASS", "CODE", "DESCRIPTION", "BASE_ENCOUNTER_COST", "TOTAL_CLAIM_COST", "PAYER_COVERAGE", "REASONCODE", "REASONDESCRIPTION"]
        },
        {
            "table": "ODS_IMMUNIZATIONS",
            "s3_path": "s3://covid19-raw-data-bucket/covid/raw/all_data/immunizations.parquet",
            "columns": ["DATE", "PATIENT", "ENCOUNTER", "CODE", "DESCRIPTION", "BASE_COST"]
        },
        {
            "table": "ODS_PATIENTS",
            "s3_path": "s3://covid19-raw-data-bucket/covid/raw/all_data/patients.parquet",
            "columns": ["ID", "BIRTHDATE", "DEATHDATE", "SSN", "DRIVERS", "PASSPORT", "PREFIX", "FIRST", "LAST", "SUFFIX", "MAIDEN", "MARITAL", "RACE", "ETHNICITY", "GENDER", "BIRTHPLACE", "ADDRESS", "CITY", "STATE", "COUNTY", "ZIP", "LAT", "LON", "HEALTHCARE_EXPENSES", "HEALTHCARE_COVERAGE"]
        },
        {
            "table": "ODS_ALLERGIES",
            "s3_path": "s3://covid19-raw-data-bucket/covid/raw/all_data/allergies.parquet",
            "columns": ["START", "STOP", "PATIENT", "ENCOUNTER", "CODE", "DESCRIPTION"]
        },
        {
            "table": "ODS_CAREPLANS",
            "s3_path": "s3://covid19-raw-data-bucket/covid/raw/all_data/careplans.parquet",
            "columns": ["ID", "START", "STOP", "PATIENT", "ENCOUNTER", "CODE", "DESCRIPTION", "REASONCODE", "REASONDESCRIPTION"]
        },
        {
            "table": "ODS_CONDITIONS",
            "s3_path": "s3://covid19-raw-data-bucket/covid/raw/all_data/conditions.parquet",
            "columns": ["START", "STOP", "PATIENT", "ENCOUNTER", "CODE", "DESCRIPTION"]
        },
        {
            "table": "ODS_ORGANIZATIONS",
            "s3_path": "s3://covid19-raw-data-bucket/covid/raw/all_data/organizations.parquet",
            "columns": ["ID", "NAME", "ADDRESS", "CITY", "STATE", "ZIP", "LAT", "LON", "PHONE", "REVENUE", "UTILIZATION"]
        },
        {
            "table": "ODS_PROCEDURES",
            "s3_path": "s3://covid19-raw-data-bucket/covid/raw/all_data/procedures.parquet",
            "columns": ["DATE", "PATIENT", "ENCOUNTER", "CODE", "DESCRIPTION", "BASE_COST", "REASONCODE", "REASONDESCRIPTION"]
        },
        {
            "table": "ODS_IMAGING_STUDIES",
            "s3_path": "s3://covid19-raw-data-bucket/covid/raw/all_data/imaging_studies.parquet",
            "columns": ["ID", "DATE", "PATIENT", "ENCOUNTER", "BODYSITE_CODE", "BODYSITE_DESCRIPTION", "MODALITY_CODE", "MODALITY_DESCRIPTION", "SOP_CODE", "SOP_DESCRIPTION"]
        },
        {
            "table": "ODS_MEDICATIONS",
            "s3_path": "s3://covid19-raw-data-bucket/covid/raw/all_data/medications.parquet",
            "columns": ["START", "STOP", "PATIENT", "PAYER", "ENCOUNTER", "CODE", "DESCRIPTION", "BASE_COST", "PAYER_COVERAGE", "DISPENSES", "TOTALCOST", "REASONCODE", "REASONDESCRIPTION"]
        },
        {
            "table": "ODS_PAYER_TRANSITIONS",
            "s3_path": "s3://covid19-raw-data-bucket/covid/raw/all_data/payer_transitions.parquet",
            "columns": ["PATIENT", "START_YEAR", "END_YEAR", "PAYER", "OWNERSHIP"]
        },
        {
            "table": "ODS_PAYERS",
            "s3_path": "s3://covid19-raw-data-bucket/covid/raw/all_data/payers.parquet",
            "columns": ["ID", "NAME", "ADDRESS", "CITY", "STATE_HEADQUARTERED", "ZIP", "PHONE", "AMOUNT_COVERED", "AMOUNT_UNCOVERED", "REVENUE", "COVERED_ENCOUNTERS", "UNCOVERED_ENCOUNTERS", "COVERED_MEDICATIONS", "UNCOVERED_MEDICATIONS", "COVERED_PROCEDURES", "UNCOVERED_PROCEDURES", "COVERED_IMMUNIZATIONS", "UNCOVERED_IMMUNIZATIONS", "UNIQUE_CUSTOMERS", "QOLS_AVG", "MEMBER_MONTHS"]
        },
        {
            "table": "ODS_PROVIDERS",
            "s3_path": "s3://covid19-raw-data-bucket/covid/raw/all_data/providers.parquet",
            "columns": ["ID", "ORGANIZATION_ID", "NAME", "GENDER", "SPECIALITY", "ADDRESS", "CITY", "STATE", "ZIP", "LAT", "LON", "UTILIZATION"]
        },
        {
            "table": "ODS_SUPPLIES",
            "s3_path": "s3://covid19-raw-data-bucket/covid/raw/all_data/supplies.parquet",
            "columns": ["DATE", "PATIENT_ID", "ENCOUNTER_ID", "CODE", "DESCRIPTION", "QUANTITY"]
        },
        {
            "table": "ODS_OBSERVATIONS",
            "s3_path": "s3://covid19-raw-data-bucket/covid/raw/all_data/observations.parquet",
            "columns": ["DATE", "PATIENT_ID", "ENCOUNTER_ID", "CODE", "DESCRIPTION", "VALUE", "UNITS", "TYPE"]
        },
        {
            "table": "ODS_DEVICES",
            "s3_path": "s3://covid19-raw-data-bucket/covid/raw/all_data/devices.parquet",
            "columns": ["START_DATE", "STOP_DATE", "PATIENT_ID", "ENCOUNTER_ID", "CODE", "DESCRIPTION", "UDI"]
        }
    ]


    # Build one big SQL string
    ods_sql = ""
    for job in copy_jobs:
        table = job['table']
        column_list = ", ".join(quote_if_reserved(col) for col in job['columns'])
        s3_path = job['s3_path']
        
        stmt = f"""
        TRUNCATE TABLE IF EXISTS BRONZE.{job['table']};

        COPY INTO BRONZE.{table}
        FROM '{job['s3_path']}'
        STORAGE_INTEGRATION = s3_int
        FILE_FORMAT = (TYPE = 'PARQUET')
        MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
        ON_ERROR = 'ABORT_STATEMENT'
        FORCE = TRUE;
        """
        ods_sql += stmt + "\n"

    try:
        with engine.connect() as conn:
            for stmt in ods_sql.strip().split(';'):
                stmt = stmt.strip()
                if stmt:
                    print(f"Executing:\n{stmt}\n")
                    conn.execute(text(stmt))
    except Exception as e:
        print("❌ Error inserting data into ODS tables in BRONZE schema:", e)
        raise

if __name__ == "__main__":
    load_ods_data()

    
