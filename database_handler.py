from multiprocessing import Pool, cpu_count
import threading
import pandas as pd
from sqlalchemy import MetaData, Table, create_engine, NullPool
from sqlalchemy.orm import sessionmaker
from connect_db import conn_alchemy_with_url, get_url, open_config
from utils import spinner,input_source, create_table_from_dataframe



class DatabaseHandler:
    """ Main class to connect to the postgres database and handle imports of
    .csv files as well as simple operations on tables. 
    The process_csv_file() method is the public method for importation.
    A user would typically be asked wether or not the .csv file is large (>1Go)
    or small. We then would have to set the DatabaHandler flag attribute to 
    True for imports of small files and False for larger files.
    """
    def __init__(self):
        self.table_name = '' 
        self.table = None
        self.df = None
        self.csv_file_path = '' 
        self.flag = None 
        self.sql_engine = None 
        self.session = None
        self.metadata = None
        self.dtype = None# read  the first line to get the names of the columns
        self.timestamp = None
        self.dbname = ''
        self._cpu = None
        
    def connect(self):
        # Establish a connection to the database which name and associated 
        # credentials are specified in the config.json file.
        # We use SQLAlchemy python library wich can be use with pandas lib
        # conn_alchemy_with_url() is part of the utils module of the project
        try:
            self.sql_engine = conn_alchemy_with_url()
            print("Successfully Connected to the database !")
        except Exception as e:
            print(f"Failed to connect: {e}")

    def disconnect(self):
        # Disconnect the SQL engine
        if self.sql_engine is not None:
            self.sql_engine.dispose()
            print("Disconnected from the database.")
    
    def init_queries(self):
        # We use special Objects from SQLAlchemy lib to create a python
        # object Table and initialize the self.metadata and self.table 
        # attributesvalues of DatabaHandler 
        self.metadata = MetaData()
        self.metadata.reflect(bind=self.sql_engine)
        self.table = Table(self.table_name,self.metadata)

    def make_query(self,column_names):
        # Make a query in the table of the self.table field. It takes one arg
        # which is a list of string containing the names of the column to query
        # e.g db_handler.make_query(['Date','Temperature'])
        # this method requires to launch init_queries first but can handle 
        # the case if that was not done.
        if self.table is not None:
            columns = [getattr(self.table.columns,col_name) for col_name in column_names]
        else:
            self.init_queries()
            columns = [getattr(self.table.columns,col_name) for col_name in column_names]

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
        self.df.to_sql(self.table_name,self.sql_engine,index=False,if_exists='replace')

    
    def _process_single_file(self):
        # Process a .csv file sequentially using a simple call to 
        self.df = pd.read_csv(self.csv_file_path)
        self.create_table_from_dataframe()

    def _write_chunk_to_sql(self, chunk, table_name, sql_engine):
        try:
            chunk.to_sql(table_name, sql_engine, if_exists='append', index=False)
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
            processed = chunk.to_sql(tab_name,engine, if_exists=mode,index=False)
            print("Chunk added to database:", chunk)
            return processed
        
        except Exception as e:
            print(e)
    
    def create_table_from_dataframe(self):
#the insert query schema is replace by pandas to_sql() method
        try:
            self.df.to_sql(self.table_name,self.sql_engine, if_exists='replace',index=False)
            print("Table has been successfully created")
        except pd.errors.DatabaseError as e:
            print("Pandas DataFrame to_sql operation failed:",e)
        except  Exception as e:
            print("an unexpected error occurred",e)


    def _process_in_chunks(self):
        try:
            chunksize = input("Enter a chunk size (default is 50000 lines): ") or 50000  
            print(chunksize)
            done_event = threading.Event()
            spinner_thread = threading.Thread(target=spinner, args=[done_event])
            spinner_thread.start()

                # inspector = inspect(self.sql_engine)
                # table_exists = self.table_name in inspector.get_table_names()
            with Pool(self._cpu) as pool:
                # for i, chunk in enumerate(pd.read_csv(self.csv_file_path, chunksize=int(chunksize),parse_dates=[self.timestamp],dtype=self.dtype)):
                # print(chunk)
                # print(f"Processing chunk {i+1} of {chunksize} lines")
                table_name = self.table_name
                chunks =pd.read_csv(self.csv_file_path, chunksize=int(chunksize))
                for chunk in chunks:
                    result = pool.apply_async(self.multiprocessing_import,args=(chunk,table_name,'append'))
                # pools+= [result.get()]
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
            print(f"Error fetching available tables: {e}")

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
    table_name, csv_file_path, flag = input_source()
    db_handler = DatabaseHandler()
    db_handler.flag = True if flag == 'y' else False
    db_handler.init_import(table_name, csv_file_path, flag)
    db_handler.connect()
    # db_handler.init_queries()
    # db_handler.make_query(['npv_const','nstr_const'])
    input('process csv?')
    db_handler.process_csv_file()
    db_handler.disconnect()

