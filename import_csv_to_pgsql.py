import csv
from connect_db import connect_to_db, open_config, print_rows

default_table_name = 'meteo_data'
default_csv_path = "../data/meteo_data.csv"

table_name = input(f"choose a name for this table: "
                   f"(default:{default_table_name})") or default_table_name


csv_file_path = input(f"enter the path to the csv file:"
                      f"(default {default_csv_path})") or default_csv_path


dbname, user, password, host, port = open_config()

conn, cursor =  connect_to_db(dbname,user,password,host,port)

with open(csv_file_path,'r') as f:
    reader = csv.reader(f)
    colum_names = next(reader)

    data_types = ['TIMESTAMP'] + ['FLOAT'] * (len(colum_names) - 1)
    sql_command = ', '.join(([f'{name} {data_type}' 
                              for name,data_type in zip(colum_names,data_types)])) 


    #first create the table: we need a SQL command like this => CREATE TABLE my_table_name (field1 data_type1, field2 data_type2, ...);
    cursor.execute(f"CREATE TABLE IF NOT EXISTS {table_name} ({sql_command})")


#the insert query schema is for example: INSERT INTO my_table_name (id, name, age) VALUES ( %s, %s, %s), (1,"bob", 25)
# the execute method allows to pass the list of values directly
    insert_query = f"INSERT INTO {table_name} ({', '.join(colum_names)}) VALUES ({', '.join(['%s'] * len(colum_names))})"

#just to check in the console that everything is good:
    for row in reader:
        cursor.execute(insert_query,row)
    
# for Debug or double check purpose
print_rows(table_name,cursor)

commit = input("Do you want to commit? 'y or n'")

if commit.strip() == 'y':
    conn.commit()

else:

    print("aborting")
    exit(1)

if conn:
    if cursor:
        cursor.close()
    conn.close()

