import psycopg2
import csv
import json


conn = None
cursor = None

with open('config.json') as fd:
    config = json.load(fd)
    config_db = config['database']
    dbname = config_db['dbname']
    user = config_db["user"]
    password = config_db["password"] 
    host = config_db["host"]
    port = config_db["port"]

default_table_name = 'meteo_data'

TABLE_NAME = input(f"choose a name for this table: (default:{default_table_name})") or default_table_name

default_csv_path = "../data/meteo_data.csv"

csv_file_path = input(f"enter the path to the csv file: (default {default_csv_path})") or default_csv_path

try :
    conn = psycopg2.connect(
            dbname=dbname,
            user=user,
            password=password,
            host=host,
            port=port
            )

    cursor = conn.cursor()

    with open(csv_file_path,'r') as f:
        reader = csv.reader(f)
        colum_names = next(reader)

        data_types = ['TIMESTAMP'] + ['FLOAT'] * (len(colum_names) - 1)
        sql_command = ', '.join(([f'{name} {data_type}' for name,data_type in zip(colum_names,data_types)])) 


        #first create the table: we need a SQL command like this => CREATE TABLE my_table_name (field1 data_type1, field2 data_type2, ...);
        cursor.execute(f"CREATE TABLE IF NOT EXISTS {TABLE_NAME} ({sql_command})")
    

#the insert query schema is for example: INSERT INTO my_table_name (id, name, age) VALUES ( %s, %s, %s), (1,"bob", 25)
# the execute method allows to pass the list of values directly
        insert_query = f"INSERT INTO {TABLE_NAME} ({', '.join(colum_names)}) VALUES ({', '.join(['%s'] * len(colum_names))})"

#just to check in the console that everything is good:
        for row in reader:
            cursor.execute(insert_query,row)
        
        cursor.execute(f"SELECT * FROM {TABLE_NAME}")
    rows = cursor.fetchall()
    if rows:
        for row in rows:
            print(row)

    else:

        print('the table is empty')

    commit = input("Do you want to commit? 'y or n'")

    if commit.strip() == 'y':
        conn.commit()

    else:

        print("aborting")
        exit(1)

except psycopg2.Error as e:

    print("Error connecting to PostgreSQL",e)
finally:
    if conn:
        if cursor:
            cursor.close()
        conn.close()

