from multiprocessing import Pool
import threading

import pandas as pd

from connect_db import conn_alchemy_with_url
from utils import multiprocessing_import, spinner, input_source,  create_table_from_dataframe, create_table_from_dataframe_in_chunks



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
        'object' if col.startswith('flag_') else
        'object' if pd.isna(col) else
        'object' if col.startswith('mpp_int') else
        'object' if col == 'mpp' else
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
    pools = []
    try:
        chunksize = input("enter a chunksize:(default is 500000 lines)\n") or 500000
        done_event = threading.Event()
        spinner_thread = threading.Thread(target=spinner,args=[done_event])
        spinner_thread.start()
        with Pool(3) as pool:
            chunks =pd.read_csv(csv_file_path, chunksize=int(chunksize))
            for chunk in chunks:
                result = pool.apply_async(multiprocessing_import,args=(chunk,table_name,'append'))
                # pools+= [result.get()]
                print(pools)
        pool.close()
        pool.join()
        print(f"Table successfully created from {csv_file_path}") 
        done_event.set()  # Signal the spinner thread to stop
        spinner_thread.join()
    except Exception as e:
        print("Error in multiprocessing call",e)
# for Debug or double check purpose
sql_engine.dispose()

