import pandas as pd
from connect_db import conn_alchemy_with_url
from utils import input_source,  create_table_from_dataframe, create_table_from_dataframe_in_chunks

table_name, csv_file_path, flag = input_source()


flag_helio_data = False


sql_engine =  conn_alchemy_with_url()

# read  the first line to get the names of the columns
dataframe = pd.read_csv(csv_file_path,nrows=1)

# ==== here we cast the columns to the correct data type ===
column_names = dataframe.columns.to_list()
timestamp = 'Date' if flag else 'ts'

data_types = {
    col: (
        'float64' if col.lower() != 'date' else 'object'
    ) if flag else (
        'object' if col.lower() in ['date', 'ts'] else
        'boolean' if col.startswith('flag_') else
        'object' if pd.isna(col) else
        'Int16' if col.startswith('mpp_int') else
        'string' if col == 'mpp' else
        'float64'
    )
    for col in column_names
}
# we read again the entire file this time using a special parser for the date
if flag:
    dataframe = pd.read_csv(csv_file_path, parse_dates=[timestamp], dtype=data_types,skiprows=0)
    print(dataframe.head(10))
    input('continue?')
    create_table_from_dataframe(dataframe, table_name,sql_engine)
else:
    for chunk in pd.read_csv(csv_file_path, chunksize=50000):
        mode = 'replace' if chunk.index[0] == 0 else 'append'

        create_table_from_dataframe_in_chunks(chunk,table_name,sql_engine,mode=mode)
    print(f"Table successfully created from {csv_file_path}") 
# for Debug or double check purpose
sql_engine.dispose()

