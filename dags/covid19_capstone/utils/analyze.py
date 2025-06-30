import pandas as pd
import numpy as np
from sqlalchemy import create_engine

# Snowflake connection details
sf_user = 'ASHWINI'
sf_password = 'VE89b2CYDC86Qew'
sf_account = 'LYEYLDE-OE27924'
sf_warehouse = 'COMPUTE_WH'
sf_database = 'COVID19'
sf_schema = 'BRONZE'
sf_role = 'ACCOUNTADMIN'

# Connect to Snowflake
engine = create_engine(
    f'snowflake://{sf_user}:{sf_password}@{sf_account}/{sf_database}/{sf_schema}'
    f'?warehouse={sf_warehouse}&role={sf_role}',
    future=True
)

# Function to load data from a table
def load_ods_table(table_name):
    query = f'SELECT * FROM {sf_schema}."{table_name}"'
    df = pd.read_sql(query, engine)
    return df

# Function to generate EDA report including IQR-based outlier detection
def eda_report(df, table_name):
    print(f"\n📊 EDA for Table: {table_name}")
    print("="*60)

    # 1. Shape
    print("🧱 Shape of the DataFrame:")
    print(df.shape)

    # 2. Data Types & Missing Values
    print("\n📄 Info:")
    print(df.info())

    print("\n🕳️ Missing Values:")
    print(df.isnull().sum()[df.isnull().sum() > 0])

    # 3. Duplicates
    print("\n📌 Duplicate Rows:", df.duplicated().sum())

    # 4. Descriptive Stats
    print("\n📊 Descriptive Statistics:")
    print(df.describe(include='all').T)

    # 5. Data Types Summary
    print("\n🧬 Data Types:")
    print(df.dtypes.value_counts())

    # 6. Value counts for categorical features
    cat_cols = df.select_dtypes(include='object').columns
    if len(cat_cols) > 0:
        print("\n🗂️ Categorical Value Counts:")
        for col in cat_cols:
            print(f"\n{col}:\n", df[col].value_counts().head())

    # 7. Outlier detection using IQR
    print("\n🚨 Outliers (IQR Method):")
    num_cols = df.select_dtypes(include=np.number).columns
    print("Numerical columns found:", list(num_cols))
    for col in num_cols:
        Q1 = df[col].quantile(0.25)
        Q3 = df[col].quantile(0.75)
        IQR = Q3 - Q1
        lower_bound = Q1 - 1.5 * IQR
        upper_bound = Q3 + 1.5 * IQR
        outliers = df[(df[col] < lower_bound) | (df[col] > upper_bound)][col]
        outlier_count = outliers.count()
        if outlier_count > 0:
            print(f"{col}: {outlier_count} outliers")
        else:
            print(f"{col}: No outliers")

# Load table and run EDA
df = load_ods_table("ODS_ALLERGIES")
eda_report(df, "ODS_ALLERGIES")
