import pandas as pd
from sqlalchemy import create_engine

# --- Snowflake connection setup ---
sf_user = 'PRAGATI26'
sf_password = 'KhGJZf9nadNdcth'
sf_account = 'DCXDSBD-DC67829'
sf_warehouse = 'COMPUTE_WH'
sf_database = 'COVID19'
sf_role = 'ACCOUNTADMIN'

# BRONZE connection
bronze_engine = create_engine(
    f'snowflake://{sf_user}:{sf_password}@{sf_account}/{sf_database}/BRONZE'
    f'?warehouse={sf_warehouse}&role={sf_role}',
    future=True
)

# SILVER connection
silver_engine = create_engine(
    f'snowflake://{sf_user}:{sf_password}@{sf_account}/{sf_database}/SILVER'
    f'?warehouse={sf_warehouse}&role={sf_role}',
    future=True
)


# --- Load table from BRONZE ---
def load_ods_table(table_name):
    query = f'SELECT * FROM BRONZE."{table_name}"'
    df = pd.read_sql(query, bronze_engine)
    return df


# --- Data Cleaning Function for ODS_DEVICES ---
def clean_devices_data(df):
    df = df.copy()

    df.drop(columns=['business_date'], inplace=True)
    # Convert to datetime
    df["start_date"] = pd.to_datetime(df["start_date"], errors="coerce")
    df["stop_date"] = pd.to_datetime(df["stop_date"], errors="coerce")

    # Handle missing stop_date
    # or keep NaT and add an "active" flag
    df["stop_date"].fillna(pd.to_datetime("2099-12-31"), inplace=True)

    # Optionally flag active devices
    df["device_active"] = df["stop_date"] == pd.to_datetime("2099-12-31")

    # Strip whitespace from categorical fields
    df["description"] = df["description"].str.strip()

    # Ensure no duplicates (you said none, but good to double-check)
    df.drop_duplicates(inplace=True)

    # Enforce uniqueness on UDI if required
    assert df["udi"].is_unique, "UDI values are not unique!"

    return df


def clean_global_daily_data(df):
    df = df.copy()

    df.drop(columns=['business_date'], inplace=True)
    # Clean date formats
    df['date_reported'] = pd.to_datetime(df['date_reported'], errors='coerce')

    # Remove duplicates
    df.drop_duplicates(inplace=True)

    # Replace negative values in new_cases and new_deaths with NaN
    df.loc[df['new_cases'] < 0, 'new_cases'] = pd.NA
    df.loc[df['new_deaths'] < 0, 'new_deaths'] = pd.NA

    # Fill missing new_cases and new_deaths with interpolation by country
    df['new_cases'] = df.groupby('country')['new_cases'].transform(
        lambda x: x.interpolate())
    df['new_deaths'] = df.groupby(
        'country')['new_deaths'].transform(lambda x: x.interpolate())

    return df


def clean_global_data(df):
    df = df.copy()

    # constant business_date removal
    df = df.drop(columns='business_date', inplace=True)

    #  Convert date_reported to datetime
    df['date_reported'] = pd.to_datetime(df['date_reported'], errors='coerce')

    #  Replace negative new_cases and new_deaths with NaN
    df.loc[df['new_cases'] < 0, 'new_cases'] = pd.NA
    df.loc[df['new_deaths'] < 0, 'new_deaths'] = pd.NA

    # Interpolate missing new_cases and new_deaths within each country
    df['new_cases'] = df.groupby('country')['new_cases'].transform(
        lambda x: x.interpolate())
    df['new_deaths'] = df.groupby(
        'country')['new_deaths'].transform(lambda x: x.interpolate())

    # Fill missing who_region using forward and backward fill per country
    df['who_region'] = df.groupby('country')['who_region'].transform(
        lambda x: x.fillna(method='ffill').fillna(method='bfill'))

    #  Remove duplicates
    df.drop_duplicates(inplace=True)

    return df


def clean_global_table_data(df):
    df = df.copy()

    #  Drop constant column
    df = df.drop(columns='business_date', inplace=True)

    #  Drop column with 100% missing values
    if df['deaths_last_7_days_per_100k'].isnull().all():
        df.drop(columns='deaths_last_7_days_per_100k', inplace=True)

    #  Fill missing who_region using forward/backward fill by country name
    df['who_region'] = df.groupby('name')['who_region'].transform(
        lambda x: x.ffill().bfill())

    #  Interpolate missing numeric values per row (optional — only if meaningful)
    numeric_cols = df.select_dtypes(include=['float64', 'int64']).columns
    df[numeric_cols] = df[numeric_cols].interpolate(method='linear', axis=0)

    #  Remove duplicates
    df.drop_duplicates(inplace=True)

    return df


def clean_hosp_icu_data(df):
    df = df.copy()

    # Convert date columns to datetime
    df["date_reported"] = pd.to_datetime(df["date_reported"], errors="coerce")

    # Drop rows where all four metrics are missing
    metric_cols = [
        "covid_new_hospitalizations_last_7days",
        "covid_new_icu_admissions_last_7days",
        "covid_new_hospitalizations_last_28days",
        "covid_new_icu_admissions_last_28days"
    ]
    df.dropna(subset=metric_cols, how="all", inplace=True)

    # Fill missing metric values with 0
    df[metric_cols] = df[metric_cols].fillna(0)

    # Remove extreme ICU outliers using simple upper threshold
    df = df[df["covid_new_icu_admissions_last_28days"] < 10000]
    df = df[df["covid_new_icu_admissions_last_7days"] < 5000]

    #  Drop constant column
    df = df.drop(columns='business_date', inplace=True)

    return df


def clean_monthly_death_by_age_data(df):
    df = df.copy()

    # Drop redundant column
    df = df.drop(columns=["business_date"])

    # Drop rows where deaths is missing (core metric)
    df = df.dropna(subset=["deaths"])

    # Fill missing who_region and wb_income with 'Unknown'
    df["who_region"] = df["who_region"].fillna("N/A")
    df["wb_income"] = df["wb_income"].fillna("N/A")

    # Cap outlier deaths (based on observed max)
    df = df[df["deaths"] <= 100000]

    return df


def clean_observations_data(df):
    df = df.copy()

    # Drop redundant column
    df = df.drop(columns=["business_date"])

    # rename the column
    df = df.rename(columns={"date": "date_value"})

    # Drop exact duplicate rows
    df = df.drop_duplicates()

    # Handle missing encounter_id: fill with 'UNKNOWN' (if needed for joins)
    df["encounter_id"] = df["encounter_id"].fillna("UNKNOWN")

    # Fill missing units with 'NA' (or leave if specific analysis requires blank units)
    df["units"] = df["units"].fillna("N/A")

    return df


def clean_covid_statewise_data(df):
    df = df.copy()

    df = df.rename(columns={"date": "date_value"})

    # Step 1: Drop columns with >85% null values
    null_threshold = 0.85
    df = df.loc[:, df.isnull().mean() < null_threshold]

    # Step 2: Define key columns to keep and clean
    key_metrics = ['death', 'hospitalized',
                   'positive', 'recovered', 'total_test_results']

    # Ensure these columns exist before trying to fill
    available_keys = [col for col in key_metrics if col in df.columns]

    # Step 3: Sort by state and date to prepare for groupby fill
    df = df.sort_values(by=['state', 'date'])

    # Step 4: Forward and backward fill nulls in key metrics per state
    df[available_keys] = df.groupby('state')[available_keys].ffill().bfill()

    # Step 5: Fill remaining nulls in those columns with 0 (just in case)
    df[available_keys] = df[available_keys].fillna(0)

    print(" Cleaned COVID data: dropped sparse columns, filled key metrics")
    return df


def clean_imaging_studies_data(df):
    df = df.copy()

    df.drop(columns=['business_date'], inplace=True)
    # Rename 'date' to 'imaging_date'
    df.rename(columns={'date': 'imaging_date'}, inplace=True)

    # Convert date columns to datetime
    df['imaging_date'] = pd.to_datetime(df['imaging_date'], errors='coerce')

    # Optional: Standardize text fields (e.g., lowercase modality_description)
    df['modality_description'] = df['modality_description'].str.title()
    df['sop_description'] = df['sop_description'].str.title()

    print(" Cleaned imaging studies data (renamed date, parsed datetimes, standardized text)")
    return df


def clean_allergies_data(df):
    df = df.copy()

    # Strip all string columns safely
    str_cols = df.select_dtypes(include='object').columns
    for col in str_cols:
        df[col] = df[col].astype(str).str.strip()
        df.loc[df[col] == 'nan', col] = pd.NA

    df = df.drop(columns=["STOP", "business_date"])

    # Convert date columns
    df['START'] = pd.to_datetime(df['START'], errors='coerce')

    # Rename columns
    df.rename(columns={
        'START': 'allergy_start_date',
        'patient': 'patient_id',
        'encounter': 'encounter_id',
        'code': 'allergy_code',
        'description': 'allergy_description',
    }, inplace=True)

    # Drop rows with null `allergy_start_date` or `load_date`
    df = df.dropna(subset=['allergy_start_date'])

    print(" Cleaned allergies data (renamed columns, parsed dates, stripped strings)")
    return df


def clean_immunizations_data(df):
    df = df.copy()

    df.drop(columns=['business_date'], inplace=True)
    # Strip whitespace safely
    str_cols = df.select_dtypes(include='object').columns
    for col in str_cols:
        df[col] = df[col].astype(str).str.strip()
        df.loc[df[col] == 'nan', col] = pd.NA

    # Convert date columns to datetime
    df['date'] = pd.to_datetime(df['date'], errors='coerce')

    # Rename columns for clarity
    df.rename(columns={
        'date': 'immunization_date',
        'patient': 'patient_id',
        'encounter': 'encounter_id',
        'code': 'immunization_code',
        'description': 'immunization_description',
        'base_cost': 'cost',
        'business_date': 'load_date'
    }, inplace=True)

    # Drop rows with null immunization_date or load_date
    df = df.dropna(subset=['immunization_date', 'load_date'])

    # Ensure cost is float
    df['cost'] = df['cost'].astype(float)

    print("Cleaned immunizations data (renamed columns, parsed dates, stripped strings)")
    return df


def clean_medications_data(df):
    df = df.copy()

    df.drop(columns=['business_date'], inplace=True)
    # Rename for clarity
    df.rename(columns={
        'START': 'medication_start_date',
        'stop': 'medication_stop_date'
    }, inplace=True)

    # Convert date columns
    df['medication_start_date'] = pd.to_datetime(
        df['medication_start_date'], errors='coerce')
    df['medication_stop_date'] = pd.to_datetime(
        df['medication_stop_date'], errors='coerce')

    # Add missing value flags (no fill)
    df['missing_stop_date_flag'] = df['medication_stop_date'].isna().astype(int)
    df['missing_reason_flag'] = df['reasoncode'].isna().astype(int)

    # Optional: Standardize text casing
    df['description'] = df['description'].str.title()
    df['reasondescription'] = df['reasondescription'].str.title()

    # Drop duplicates
    df.drop_duplicates(inplace=True)

    print("Cleaned medications data with missing flags (no fill).")
    return df


def clean_organizations_data(df):
    df = df.copy()

    df.drop(columns=['business_date'], inplace=True)
    # Optional: Add flag for missing phone numbers
    df['has_phone'] = df['phone'].notna()

    print(" Cleaned organizations data (parsed business_date, added has_phone flag)")
    return df


def clean_patients_data(df):
    df = df.copy()

    df = df.drop(columns=["business_date", "SUFFIX",
                 "MAIDEN", "DRIVERS", "PASSPORT", "SSN", "ADDRESS"])

    df['is_deceased'] = df['deathdate'].notnull().astype(int)

    df['drivers'].fillna('N/A', inplace=True)
    df['passport'].fillna('N/A', inplace=True)
    df['prefix'].fillna('N/A', inplace=True)
    df['marital'].fillna('N/A', inplace=True)
    df['zip'].fillna('00000', inplace=True)

    df['birthdate'] = pd.to_datetime(df['birthdate'], errors='coerce')
    df['deathdate'] = pd.to_datetime(df['deathdate'], errors='coerce')

    # Date parsing
    for col in ['birthdate', 'deathdate']:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce')

    # Remove duplicates by ID if present
    if 'id' in df.columns:
        df = df.drop_duplicates(subset='id')
        print(" Removed duplicates using column: id")


def clean_payers_data(df):
    df = df.copy()

    df = df.drop(columns=["business_date"], inplace=True)

    # Add missing value flags (no fill)
    df['missing_address_flag'] = df['address'].isna().astype(int)
    df['missing_city_flag'] = df['city'].isna().astype(int)
    df['missing_state_flag'] = df['state_headquartered'].isna().astype(int)
    df['missing_zip_flag'] = df['zip'].isna().astype(int)
    df['missing_phone_flag'] = df['phone'].isna().astype(int)

    # Optional: standardize casing
    df['name'] = df['name'].str.title()
    df['city'] = df['city'].str.title()
    df['state_headquartered'] = df['state_headquartered'].str.upper()

    # Drop duplicates just in case
    df.drop_duplicates(inplace=True)

    print(" Cleaned payers data with missing value flags.")
    return df


def clean_encounters_data(df):
    df = df.copy()

    df = df.drop(columns=["business_date"], inplace=True)

    # Rename columns
    df.rename(columns={
        'START': 'start_value',
        'stop': 'stop_value'
    }, inplace=True)

    print(" Cleaned encounters data (renamed columns and parsed dates)")
    return df


def clean_procedures_data(df):
    df = df.copy()

    df = df.drop(columns=["business_date"], inplace=True)

    # Rename 'date' to 'procedure_date'
    df.rename(columns={'date': 'procedure_date'}, inplace=True)

    # Convert date columns to datetime
    df['procedure_date'] = pd.to_datetime(
        df['procedure_date'], errors='coerce')
    df['business_date'] = pd.to_datetime(df['business_date'], errors='coerce')

    # Add flag for missing reason
    df['has_reason'] = df['reasoncode'].notna()

    print(" Cleaned procedures data (renamed date, parsed datetimes, added has_reason flag)")
    return df


def clean_providers(df):
    df = df.copy()

    df = df.drop(columns=["business_date"], inplace=True)

    # Standardize text formats
    df['gender'] = df['gender'].str.upper(
    ).str.strip()            # e.g., 'F', 'M'
    df['speciality'] = df['speciality'].str.upper().str.strip()    # For uniformity

    # Drop duplicates if any (EDA shows 0)
    df.drop_duplicates(inplace=True)

    print(" Cleaned providers data.")
    return df


def clean_supplies(df):
    df = df.copy()

    df = df.drop(columns=["business_date"], inplace=True)

    # Rename columns
    df.rename(columns={'date': 'supply_date'}, inplace=True)

    # Convert to datetime
    df['supply_date'] = pd.to_datetime(df['supply_date'], errors='coerce')

    # Standardize descriptions
    df['description'] = df['description'].str.strip().str.title()

    # Drop duplicates
    df.drop_duplicates(inplace=True)

    print(" Cleaned supplies data (with renamed columns).")
    return df


def clean_ods_vaccination_data(df: pd.DataFrame) -> pd.DataFrame:
    # Step 1: Ensure consistent column names (optional: strip spaces, lowercase)
    df.columns = df.columns.str.strip().str.lower()

    df.drop(columns=['business_date'], inplace=True)
    # Step 2: Convert date columns to datetime
    df['date_updated'] = pd.to_datetime(df['date_updated'], errors='coerce')
    df['first_vaccine_date'] = pd.to_datetime(
        df['first_vaccine_date'], errors='coerce')

    # Step 3: Handle missing values
    # - Fill 'who_region' with 'UNKNOWN'
    df['who_region'] = df['who_region'].fillna('UNKNOWN')

    # - For numerical vaccination fields, fill with 0 or keep NaNs for imputation
    numeric_cols = [
        'total_vaccinations',
        'persons_vaccinated_1plus_dose',
        'total_vaccinations_per100',
        'persons_vaccinated_1plus_dose_per100',
        'persons_last_dose',
        'persons_last_dose_per100',
        'persons_booster_add_dose',
        'persons_booster_add_dose_per100'
    ]
    df[numeric_cols] = df[numeric_cols].fillna(0)

    # - Fill missing date_updated with business_date
    df['date_updated'] = df['date_updated'].fillna(df['business_date'])

    # Step 4: Add data quality flags
    df['is_missing_vaccine_info'] = df['vaccines_used'].isna(
    ) | df['number_vaccines_types_used'].isna()
    df['has_booster_data'] = df['persons_booster_add_dose'] > 0

    # Step 5: Drop fully empty columns (e.g., 'vaccines_used' and 'number_vaccines_types_used' are all null)
    df = df.drop(columns=['vaccines_used', 'number_vaccines_types_used'])

    # Step 6: Drop duplicates if any
    df = df.drop_duplicates()

    return df


def clean_careplans_data(df):
    df = df.copy()

    df.drop(columns=['business_date'], inplace=True)
    # Rename columns for consistency
    df.rename(columns={
        'START': 'start_date',
        'stop': 'end_date'
    }, inplace=True)

    # Convert date columns
    df['start_date'] = pd.to_datetime(df['start_date'], errors='coerce')
    df['end_date'] = pd.to_datetime(df['end_date'], errors='coerce')

    # Add is_active flag for ongoing care plans
    df['is_active'] = df['end_date'].isna()

    print("Cleaned careplans data (renamed columns, parsed dates, added is_active)")
    return df


def clean_conditions_data(df):
    df = df.copy()

    df = df.drop(columns=["business_date"], inplace=True)

    # Rename columns for consistency
    df.rename(columns={
        'START': 'start_date',
        'stop': 'end_date'
    }, inplace=True)

    # Convert to datetime
    df['start_date'] = pd.to_datetime(df['start_date'], errors='coerce')
    df['end_date'] = pd.to_datetime(df['end_date'], errors='coerce')

    # Add is_active column based on missing end_date
    df['is_active'] = df['end_date'].isna()

    print("Cleaned conditions data (renamed columns, parsed dates, added is_active)")
    return df


def clean_payer_transitions(df):
    df = df.copy()

    df = df.drop(columns=["business_date"], inplace=True)

    # Add missing value flag for ownership
    df['missing_ownership_flag'] = df['ownership'].isna().astype(int)

    # Optional standardizations
    df['ownership'] = df['ownership'].str.title()  # Self, Spouse, Guardian
    df.drop_duplicates(inplace=True)

    print("Cleaned payer transitions data with missing value flags.")
    return df

# --- Write to SILVER ---


def write_to_silver(df, table_name):
    df.to_sql(
        name=table_name,
        con=silver_engine,
        if_exists='replace',
        index=False,
        chunksize=10000

    )
    print(f" Cleaned data written to SILVER.{table_name}")


# --- Full Pipeline ---
if __name__ == "__main__":
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

    for table_name, clean_func in tables.items():
        df = load_ods_table(table_name)
        df_clean = clean_func(df)
        stg_table_name = f"STG_{table_name.upper()}"
        write_to_silver(df_clean, stg_table_name)
