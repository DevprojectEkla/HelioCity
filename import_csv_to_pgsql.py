from sqlalchemy import inspect,text
import pandas as pd
from multiprocessing import Pool
import threading
from connect_db import conn_alchemy_with_url
from utils import spinner,input_source, create_table_from_dataframe, create_table_from_dataframe_in_chunks

class DatabaseHandler:
    def __init__(self, table_name, csv_file_path, flag):
        self.table_name = table_name
        self.csv_file_path = csv_file_path
        self.flag = flag
        self.sql_engine = conn_alchemy_with_url()
        self.dtype = None# read  the first line to get the names of the columns
        self.timestamp = None


    def generate_types(self):
        dataframe = pd.read_csv(csv_file_path,nrows=1)

# ==== here we cast the columns to the correct data type ===
        column_names = dataframe.columns.to_list()
        self.timestamp = 'Date' if self.flag else 'ts'

        self.dtypes = {
    col: (
        'float64' if col.lower() != 'date' else 'object'
    ) if self.flag else (
        'object' if col.lower() in ['date', 'ts'] else
        'boolean' if col.startswith('flag_') else
        'object' if pd.isna(col) else
        'object' if col.startswith('mpp_int') else
        'string' if col == 'mpp' else
        'float64'
    )
    for col in column_names
}
        print(self.dtypes,self.timestamp)
        dataframe.to_sql(self.table_name,self.sql_engine,index=False,if_exists='replace')

    def process_csv_file(self):
        self.generate_types()
        if self.flag:
            self._process_single_file()
        else:
            self._process_in_chunks()

    def _process_single_file(self):
        dataframe = pd.read_csv(self.csv_file_path)
        create_table_from_dataframe(dataframe, self.table_name, self.sql_engine)

    def _write_chunk_to_sql(self,chunk, table_name, sql_engine):
        try:
            chunk.to_sql(table_name, sql_engine, if_exists='append', index=False)
        except pd.errors.DatabaseError as e:
            print("Pandas DataFrame to_sql operation failed:", e)

    def _process_in_chunks(self):
        try:
            chunksize = input("Enter a chunk size (default is 200000 lines): ") or 200000  
            print(chunksize)
            done_event = threading.Event()
            spinner_thread = threading.Thread(target=spinner, args=[done_event])
            spinner_thread.start()

                # inspector = inspect(self.sql_engine)
                # table_exists = self.table_name in inspector.get_table_names()
            for i, chunk in enumerate(pd.read_csv(self.csv_file_path, chunksize=int(chunksize),parse_dates=[self.timestamp],dtype=self.dtype)):
                print(chunk)
                print(f"Processing chunk {i+1} of {chunksize} lines")
                self._write_chunk_to_sql(chunk, self.table_name, self.sql_engine)
            print(f"Table successfully created from {self.csv_file_path}")
            done_event.set()  # Signal the spinner thread to stop
            spinner_thread.join()
        except Exception as e:
            print("Error in multiprocessing call:", e)

    def close_connection(self):
        self.sql_engine.dispose()


if __name__ == "__main__":
    table_name, csv_file_path, flag = input_source()
    db_handler = DatabaseHandler(table_name, csv_file_path, flag)
    db_handler.process_csv_file()
    db_handler.close_connection()

