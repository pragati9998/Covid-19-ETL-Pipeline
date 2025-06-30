from sqlalchemy import create_engine

# ----- Snowflake Engine -----
def get_snowflake_engine(schema):
    user = 'ASHWINI'
    password = 'VE89b2CYDC86Qew'
    account = 'LYEYLDE-OE27924'
    database = 'COVID19'
    warehouse = 'COMPUTE_WH'

    return create_engine(
        f'snowflake://{user}:{password}@{account}/{database}/{schema}?warehouse={warehouse}'
    )