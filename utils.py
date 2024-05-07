import pandas as pd

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


def create_table_from_dataframe_in_chunks(chunk,table_name,sql_engine,mode='append'):
#the insert query schema is replace by pandas to_sql() method
    try:
        if mode == 'replace':
            print("=== replace mode ====")
            chunk.to_sql(table_name,sql_engine, if_exists=mode,index=False)

            print("new tab created successfully with first chunk")
        else:
            
            print("=== append mode ====")
            chunk.to_sql(table_name,sql_engine, if_exists=mode,index=False)
            print("chunk inserted successfully")
    except pd.errors.DatabaseError as e:
        print("Pandas DataFrame to_sql operation failed:",e)
    except  Exception as e:
        print("an unexpected error occurred",e)

