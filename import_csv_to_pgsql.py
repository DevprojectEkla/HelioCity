import csv
from connect_db import connect_to_db, open_config
from utils import close_conn, input_source, print_rows,confirm_and_commit

table_name, csv_file_path, flag = input_source()

dbname, user, password, host, port = open_config()

flag_helio_data = False


conn, cursor =  connect_to_db(dbname,user,password,host,port)

with open(csv_file_path,'r') as f:
    reader = csv.reader(f)
    colum_names = next(reader)

    if flag:
        #DataTypes for the meteo API .csv file
        data_types = ['TIMESTAMP'] + ['FLOAT'] * (len(colum_names) - 1)
    else:
        #Data Types for heliocity_results.csv data file
        data_types = ['INTEGER'] + ['TIMESTAMP'] + ['INTEGER'] + ['INTEGER']+ ['NULL'] + ['VARCHAR(50)'] + ['FLOAT'] * (len(colum_names) - 6 )
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

confirm_and_commit(conn)
close_conn(conn,cursor)
