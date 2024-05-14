from functools import partial
import sys
from multiprocessing import Pool, cpu_count
from concurrent.futures import ProcessPoolExecutor
import threading
import pandas as pd
from sqlalchemy import Column, Float, MetaData, String, Table, create_engine, NullPool
from sqlalchemy.orm import sessionmaker
from connect_db import conn_alchemy_with_url, get_url, open_config
from utils import get_data_types, spinner,input_source, create_table_from_dataframe



class DatabaseHandler:
    """ Main class to connect to the postgres database and handle imports of
    .csv files as well as simple operations on tables. 
    The process_csv_file() method is the public method for importation.
    A user would typically be asked wether or not the .csv file is large (>1Go)
    or small. We then would have to set the DatabaHandler flag attribute to 
    True for imports of small files and False for larger files.
    INFO: Since we want to interact with our DB and perform CRUD operations on
    data stored in the database, SQLAlchemy's Table class provides
    an object-oriented interface for defining and interacting with database tables.
    Beware: Don't forget to call .disconnect() once you are done to avoid
            hanging process.
    """
    def __init__(self):
        self.table_name = '' 
        self.table = None
        self.columns = None
        self.df = None
        self.csv_file_path = '' 
        self.flag = None 
        self.chunksize = ''
        self.sql_engine = None 
        self.session = None
        self.metadata = None
        self.dtype = None
        self.timestamp = None
        self.dbname = ''
        self._cpu = None
        
    def connect(self):
        # Establish a connection to the database which name and associated 
        # credentials are specified in the config.json file.
        # This method must be call at the very beginning right after
        # class instantiation.
        # We use SQLAlchemy python library wich is used in combination with
        # pandas lib conn_alchemy_with_url() is part of the utils module
        # of the project
        try:
            self.sql_engine = conn_alchemy_with_url()
            print("Successfully Connected to the database !")
        except Exception as e:
            print(f"Failed to connect: {e}")

    def disconnect(self):
        # Disconnect the SQL engine. This should be called at the end of the
        # task
        if self.sql_engine is not None:
            self.sql_engine.dispose()
            print("Disconnected from the database.")

    @staticmethod
    def insert_data(chunk,table):
        print("Inserting data into the table...")
        url = get_url()
        engine = create_engine(url, poolclass=NullPool)
        engine.dispose()

        with engine.connect() as conn, conn.begin():
            for _, row in chunk.iterrows():
                print(row,table)
                conn.execute(table.insert(), **row)
                print(chunk)
                conn.commit()
        print("Data inserted successfully.")
    
    def process_table_model(self):
        self._generate_types()
        self.create_table_model_from_csv()
        self.metadata = MetaData()
        self.chunksize = input("enter chunksize:\n"
                               "default chunksize is 500") or 500 
        try:
            with pd.read_csv(self.csv_file_path,chunksize=int(self.chunksize)) as reader:
                with ProcessPoolExecutor() as executor:
                    futures = [executor.submit(self.insert_data,chunk, self.table)
                               for chunk in reader]
                    for future in futures:
                        future.result()
        except Exception as e:
            print("An error occured while processing Table model to DB"
                  "\nERROR:",e)

        

    def init_queries(self):
        # We use special Objects from SQLAlchemy lib to create a python
        # object Table and initialize the self.metadata and self.table 
        # attributesvalues of DatabaHandler 
        self.metadata = MetaData()
        self.metadata.reflect(bind=self.sql_engine)
        self.table = Table(self.table_name,self.metadata)

    def make_query(self,column_names):
        # Make a query in the table of the self.table field. 
        # It takes one argument which is a list of string containing
        # the names of the column to query
        # e.g db_handler.make_query(['Date','Temperature'])
        # this method requires to launch init_queries first but can handle 
        # the case if that was not done.
        self.columns = column_names
        if self.table is not None:
            columns = [getattr(self.table.columns,
                               col_name) for col_name in column_names]
        else:
            self.init_queries()
            columns = [getattr(self.table.columns,
                               col_name) for col_name in column_names]

        self.session = sessionmaker(bind=self.sql_engine)
        with self.session() as session:
            queries = session.query(*columns).all() 
            session.close()
        print(queries)
        return queries

    def init_import(self,table_name, csv_file_path, flag):
        # Initialize main class attributes typically retrieved from a user input
        self.table_name = table_name
        self.csv_file_path = csv_file_path
        self.flag = flag
        self.dbname = open_config()[0]
        self._cpu = cpu_count()

    
    def process_csv_file(self):
        # Main method to import a .csv file into the db.
        # Depending on the flag value it can trigger a simple function to 
        # process the file sequentially or trigger true parallelism using
        # the multiprocessing lib of Python. 
        self._generate_types()
        if self.flag:
            self._process_single_file()
        else:
            self._process_in_chunks()
    
    def _generate_types(self):
        # A private method to set the data types of our table 
        # This should be modified based on the Models of the DB
        self.df = pd.read_csv(self.csv_file_path,nrows=1)

    # ==== here we cast the columns to the correct data type ===
        column_names = self.df.columns.to_list()
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
        self.df.to_sql(self.table_name,
                       self.sql_engine,
                       index=False,
                       if_exists='replace')
    @staticmethod
    def _get_data_types(column_name, dtypes):
        dtype = dtypes.get(column_name.lower(),'object')
        print(dtype)

        if dtype == 'float64':
            return Float
        elif dtype == 'object' or dtype == 'string':
            return String


    
    def _process_single_file(self):
        # Process a .csv file sequentially using a simple call to 
        self.df = pd.read_csv(self.csv_file_path)
        self.create_table_from_dataframe()

    def _write_chunk_to_sql(self, chunk, table_name, sql_engine):
        try:
            chunk.to_sql(table_name, sql_engine,
                         if_exists='append',
                         index=False)
        except pd.errors.DatabaseError as e:
            print("Pandas DataFrame to_sql operation failed:", e)

    @staticmethod
    # la fonction à passer comme argument de Pool 
    def multiprocessing_import(chunk, tab_name, mode='append'):
        # We need an engine connection for each process
        url = get_url()
        engine = create_engine(url,poolclass=NullPool)
        engine.dispose()
        try:
            print("processing chunk")
            processed = chunk.to_sql(tab_name,engine,
                                     if_exists=mode,
                                     index=False)
            print("Chunk added to database:\n", chunk)
            return processed
        
        except Exception as e:
            print(e)
    
    def create_table_from_dataframe(self):
#the insert query schema is replace by pandas to_sql() method
        try:
            self.df.to_sql(self.table_name,self.sql_engine,
                           if_exists='replace',
                           index=False)
            print("Table has been successfully created")
        except pd.errors.DatabaseError as e:
            print("Pandas DataFrame to_sql operation failed:", e)
        except  Exception as e:
            print("an unexpected error occurred", e)

    
    def create_table_model_from_csv(self):
        self._generate_types()
        self.metadata = MetaData()
        df = pd.read_csv(self.csv_file_path,nrows=1)

        columns = [Column(col_name, get_data_types(col_name,self.dtypes)) 
                  for col_name in df.columns]
        self.table = Table(table_name, self.metadata, *columns)
        try:
           self.metadata.create_all(self.sql_engine)
        except Exception as e:
            print("cannot create Table with create_all method\nerror:", e)

    def _process_in_chunks(self):
        try:
            chunksize = input(
                    "Enter a chunk size (default is 500 lines): ") or 500  
            print(chunksize)
            done_event = threading.Event()
            spinner_thread = threading.Thread(target=spinner,
                                              args=[done_event])
            spinner_thread.start()

            with Pool(self._cpu) as pool:
                
                table_name = self.table_name
                chunks =pd.read_csv(self.csv_file_path,
                                    chunksize=int(chunksize))
                try:
                    # we use partial() from functools lib to fill the parameters
                    # before passing it to pool.map to which we cannot pass
                    # a list of args
                    filled_multiprocessing = partial(
                            self.multiprocessing_import,
                            tab_name=table_name,
                            mode='append')
                    result = pool.map_async(filled_multiprocessing,chunks)
                    results = result.get()
                    print(results)
                except Exception as e:
                    print("Error in multiprocessing apply_async function", e)
            pool.close()
            pool.join()

            print(f"Table successfully created from {self.csv_file_path}")
            done_event.set()  # Signal the spinner thread to stop
            spinner_thread.join()
        except Exception as e:
            print("Error in multiprocessing call:", e)
    
    def get_available_tables(self):
        try:
            query = """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'public'
            """
            df_tables = pd.read_sql_query(query, self.sql_engine)
            return df_tables['table_name'].tolist()
        except Exception as e:
            print("Error fetching available tables: ", e)

    def get_fields_in_table(self,table_name):
        try:
            query = f"""
            SELECT column_name 
            FROM information_schema.columns
            WHERE table_name = N'{table_name}'
            """

            df_columns = pd.read_sql_query(query, self.sql_engine)
            return df_columns['column_name'].tolist()

        except Exception as e:
            print("Error while trying getting fields name in the table",e)

    def fetch_all_values(self, column_names):
        # TODO
        for column in column_names:
            pass

if __name__ == "__main__":
    # You can test the file using python database_handler.py 
    simple_mode = ''
    if len(sys.argv) < 2:
        print("Usage with arguments: python script.py <table_name> <csv_file_path>")
        table_name, csv_file_path, simple_mode = input_source()
    
    elif len(sys.argv) <= 3:
        table_name = sys.argv[1]
        csv_file_path = sys.argv[2]
        print(":: multiprocessing mode import ::\n"
              "(add -f for small files and simpler import mode)")
        if len(sys.argv) == 3:
            if sys.argv[2] == '-f':
                print(":: simple import mode activated ::")
                simple_mode = 'y' 
    else:
        print("wrong number of arguments")
        simple_mode = ''
        exit(1)
        
    # Extract the argument from the command line
    db_handler = DatabaseHandler()
    flag = True if simple_mode == 'y' or simple_mode == True else False
    db_handler.connect()
    db_handler.init_import(table_name, csv_file_path, flag)
    # db_handler.init_queries()
    # db_handler.make_query(['Date','Temperature'])
    input('process csv?')
    db_handler.process_table_model()
    # db_handler.process_csv_file()
    db_handler.disconnect()

