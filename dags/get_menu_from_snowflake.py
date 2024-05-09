"""
# Get the restaurant menu list
"""

import logging

from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from snowflake.connector import DictCursor
from pendulum import datetime


MENU_PATH = Variable.get("MENU_PATH")
MENU_FILE_NAME = "menu.csv"
MENU_FULL_PATH = MENU_PATH + MENU_FILE_NAME

LOGGER = logging.getLogger("SnowflakeProvider")
LOGGER.setLevel("DEBUG")

@task
def get_menu_list():
    """Get the menu list from Snowflake and return it"""
    
    hook = SnowflakeHook(snowflake_conn_id="SNOWFLAKE_CONNECTION")
    snowflake_conn = hook.get_conn()
    cursor = snowflake_conn.cursor(DictCursor)
    sql_query = """SELECT * FROM TASTY_BYTES_SAMPLE_DATA.RAW_POS.MENU"""
    try:
        cursor.execute(sql_query)
        df = cursor.fetch_pandas_all()
        #result = result.fetchall()
        LOGGER.info(f"Display the first MENU_ID: {df['MENU_ID'][0]}")
    except Exception as e:
        LOGGER.error(f"Snowflake Exception: {e}")
    finally:
        cursor.close()
        snowflake_conn.close()
        
    return df

@task
def write_snowflake_data_in_csv_file(filename, pandas_data):
    try:
        pandas_data.to_csv(filename)
        LOGGER.info(f"Writing file <<{filename}>> is done !")
    except Exception as e:
        LOGGER.error(f"File error: {e}")

@dag(
    dag_id="get_menu_from_snowflake_dag",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    tags=["snowflake_hook"],
    doc_md=__doc__,
    description="Play SnowflakeHook",
    schedule_interval=None,    
    catchup=False,
)
def get_menu_from_snowflake_dag():
    pass

    pandas_df = get_menu_list()
    write_snowflake_data_in_csv_file(MENU_FULL_PATH, pandas_df)
    
get_menu_from_snowflake_dag()