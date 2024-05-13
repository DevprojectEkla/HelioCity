from multiprocessing import Pool, cpu_count
import threading
import pandas as pd
from sqlalchemy import MetaData, Table, create_engine, NullPool
from sqlalchemy.orm import sessionmaker
from connect_db import conn_alchemy_with_url, get_url, open_config
from utils import spinner,input_source, create_table_from_dataframe



class DatabaseHandler:
    def __init__(self):
        self.table_name = '' 
        self.table = None
        self.csv_file_path = '' 
        self.flag = ''
        self.sql_engine = None 
        self.session = None
        self.metadata = None
        self.dtype = None# read  the first line to get the names of the columns
        self.timestamp = None
        self.dbname = ''
        self.cpu = None
        
    def connect(self):
        try:
            self.sql_engine = conn_alchemy_with_url()
            print("Successfully Connected to the database !")
        except Exception as e:
            print(f"Failed to connect: {e}")

    def disconnect(self):
        if self.sql_engine is not None:
            self.sql_engine.dispose()
            print("Disconnected from the database.")
    
    def init_queries(self):

        self.metadata = MetaData()
        self.metadata.reflect(bind=self.sql_engine)
        self.table = Table(self.table_name,self.metadata)

    def make_query(self,column_names):
        columns = [getattr(self.table.columns,col_name) for col_name in column_names]

        self.session = sessionmaker(bind=self.sql_engine)
        with self.session() as session:
            queries = session.query(*columns).all() 
            session.close()
        print(queries)
        return queries

            

    def init_import(self,table_name, csv_file_path, flag):
        self.table_name = table_name
        self.csv_file_path = csv_file_path
        self.flag = flag
        self.dbname = open_config()[0]
        self.cpu = cpu_count()

    
    def process_csv_file(self):
        # this is the main method to call to import a .csv file into the db
        self._generate_types()
        if self.flag:
            self._process_single_file()
        else:
            self._process_in_chunks()
    
    
    def _generate_types(self):
        dataframe = pd.read_csv(self.csv_file_path,nrows=1)

# ==== here we cast the columns to the correct data type ===
        column_names = dataframe.columns.to_list()
        self.timestamp = 'Date' if self.flag else 'ts'

        self.dtypes = {
    col: (
        'float64' if col.lower() != 'date' else 'object'
    ) if self.flag else (
        'object' if col.lower() in ['date', 'ts'] else
        'object' if col.startswith('flag_') else
        'object' if pd.isna(col) else
        'object' if col.startswith('mpp_int') else
        'string' if col == 'mpp' else
        'float64'
    )
    for col in column_names
}
        print(self.dtypes,self.timestamp)
        dataframe.to_sql(self.table_name,self.sql_engine,index=False,if_exists='replace')

    
    def _process_single_file(self):
        dataframe = pd.read_csv(self.csv_file_path)
        create_table_from_dataframe(dataframe, self.table_name, self.sql_engine)

    def _write_chunk_to_sql(self, chunk, table_name, sql_engine):
        try:
            chunk.to_sql(table_name, sql_engine, if_exists='append', index=False)
        except pd.errors.DatabaseError as e:
            print("Pandas DataFrame to_sql operation failed:", e)

    @staticmethod
    def multiprocessing_import(chunk, tab_name, mode='append'):
        # We need an engine connection for each process
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


    def _process_in_chunks(self):
        try:
            chunksize = input("Enter a chunk size (default is 50000 lines): ") or 50000  
            print(chunksize)
            done_event = threading.Event()
            spinner_thread = threading.Thread(target=spinner, args=[done_event])
            spinner_thread.start()

                # inspector = inspect(self.sql_engine)
                # table_exists = self.table_name in inspector.get_table_names()
            with Pool(self.cpu) as pool:
                # for i, chunk in enumerate(pd.read_csv(self.csv_file_path, chunksize=int(chunksize),parse_dates=[self.timestamp],dtype=self.dtype)):
                # print(chunk)
                # print(f"Processing chunk {i+1} of {chunksize} lines")
                chunks =pd.read_csv(self.csv_file_path, chunksize=int(chunksize))
                for chunk in chunks:
                    result = pool.apply_async(self.multiprocessing_import,args=(chunk,table_name,'append'))
                # pools+= [result.get()]
                    print(result.get())
            pool.close()
            pool.join()

            print(f"Table successfully created from {self.csv_file_path}")
            done_event.set()  # Signal the spinner thread to stop
            spinner_thread.join()
        except Exception as e:
            print("Error in multiprocessing call:", e)



if __name__ == "__main__":
    table_name, csv_file_path, flag = input_source()
    db_handler = DatabaseHandler()
    db_handler.init_import(table_name, csv_file_path, flag)
    db_handler.connect()
    # db_handler.init_queries()
    # db_handler.make_query(['npv_const','nstr_const'])
    input('process csv?')
    db_handler.process_csv_file()
    db_handler.disconnect()

