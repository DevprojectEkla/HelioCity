import sys
import time
import pandas as pd
from sqlalchemy import Float, NullPool, String, create_engine

from connect_db import get_url

HIDE_CURSOR = "\033[?25l"
SHOW_CURSOR = "\033[?25h"

def print_with_alchemy(tab, engine):
    try:
        df = pd.read_sql(f"SELECT * FROM {tab}", engine)
        print(df)

    except Exception as e:
        print("Error while fetching and printing rows with engine",e)


def print_rows(table_name,cursor):
    cursor.execute(f"SELECT * FROM {table_name}")
    rows = cursor.fetchall()
    if rows:
        for row in rows:
            print(row)

    else:

        print('the table is empty')

def confirm_and_commit(conn):
    commit = input("Do you want to commit? 'y or n'")

    if commit.strip() == 'y':
        conn.commit()

    else:

        print("aborting")
        exit(1)

def close_conn(conn,cursor):
    if conn:
        if cursor:
            cursor.close()
            print("cursor successfully disconnected")
        conn.close()
        print("DB Connection closed")

def input_source():
    default_table_name = 'meteo_data'
    default_csv_path = "./data/meteo_data.csv"
    flag_meteo = False
    table_name = input(f"choose a name for this table: "
                   f"(default:{default_table_name})") or default_table_name
    csv_file_path = input(f"enter the path to the csv file:"
                      f"(default {default_csv_path})") or default_csv_path

    meteo_data = input(f"ATTENTION: do data come from the meteo API? ('y' or 'n') if not we will assume data come from HelioCity calculator\n")

    if meteo_data == 'y':
        flag_meteo = True
        print('=> meteo data pretreatment')
    else:
        print("=> calculator data posttreatment")
    

    return table_name, csv_file_path, flag_meteo

def create_table_from_dataframe(dataframe,table_name,sql_engine):
#the insert query schema is replace by pandas to_sql() method
    try:
        dataframe.to_sql(table_name,sql_engine, if_exists='replace',index=False)
        print("Table has been successfully created")
    except pd.errors.DatabaseError as e:
        print("Pandas DataFrame to_sql operation failed:",e)
    except  Exception as e:
        print("an unexpected error occurred",e)

def multiprocessing_import(chunk,tab_name,mode='append'):
    url = get_url()
    engine = create_engine(url,poolclass=NullPool)
    engine.dispose()
    print(chunk)
    try:
        print("processing chunk")
        processed = chunk.to_sql(tab_name,engine, if_exists=mode,index=False)
        return processed
        
    except Exception as e:
        print(e)

def get_data_types(column_name, dtypes):
        dtype = dtypes.get(column_name.lower(),'object')
        print(dtype)

        if dtype == 'float64':
            return Float
        elif dtype == 'object' or dtype == 'string':
            return String


def create_table_from_dataframe_in_chunks(chunk,table_name,sql_engine):
#the insert query schema is replace by pandas to_sql() method
    try:
        chunk.to_sql(table_name,sql_engine,index=False,if_exists='replace')
        print("chunk inserted successfully")
    except pd.errors.DatabaseError as e:
        print("Pandas DataFrame to_sql operation failed:",e)
    except  Exception as e:
        print("an unexpected error occurred",e)

def get_available_tables(engine):

    query = """
    SELECT table_name
    FROM information_schema.tables
    WHERE table_schema = 'public'
    """

    df_tables = pd.read_sql_query(query, engine)

    return df_tables['table_name'].tolist()

def spinner(done_event):
    spinner_chars = "|/-\\"
    i = 0
    while not done_event.is_set():
        sys.stdout.write(HIDE_CURSOR)
        print(spinner_chars[i % len(spinner_chars)], end="\r")
        i += 1
        time.sleep(0.1)
    sys.stdout.write(SHOW_CURSOR)
