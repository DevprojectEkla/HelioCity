from sqlalchemy import text
from connect_db import open_config,conn_alchemy_with_url
import pandas as pd

from database_handler import DatabaseHandler

class DatabaseSelector(DatabaseHandler):

    def __init__(self):
        super().__init__()
        self.raw_formula_list = []
        

    def select_interval(self, start, end, column_name, source_table_name, new_table_name):
        try:
            select_query = f"""
                SELECT * INTO {new_table_name}
                FROM {source_table_name}
                WHERE "{column_name}" >= '{start}' AND "{column_name}" <= '{end}'
            """
            
            with self.sql_engine.connect() as conn:
                conn.execute(text(select_query))
                conn.commit()
                print(f"Interval selected successfully and stored in table '{new_table_name}'.")
        except Exception as e:
            print(f"Error in query: {e}")

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

    def _raw_formula(self):
        self.raw_formula_list = [("Apparent_temp",'CAST((-2.7 * (1 - (rel_humidity / 100)) * (5.0 * SQRT(GREATEST(wind_speed, 0)) - 0.1) + "Temperature") AS numeric(10,2))')]

    def _python_formula(self,arg1,arg2):
        if arg1 and arg2:
            value = arg1 * arg2
        else:
            value = None

        return 'python_calc',value

    def _create_new_column(self,column_name):

        statement = f"""
        ALTER TABLE {self.table_name}
        ADD {column_name}
        FLOAT
        """
        try:
            with self.sql_engine.connect() as conn:
                conn.execute(text(statement))
                conn.commit()
                print(":: new column created ::")
        except Exception as e:
            print("Error:",e)

    def _insert_new_variable(self, formula_desc):
        col_name, formula = formula_desc
        statement = f"""
        UPDATE {self.table_name}
        SET {col_name} = {formula} 
        """
        try:
            with self.sql_engine.connect() as conn:
                conn.execute(text(statement))
                conn.commit()
                print("New Variable successfuly inserted")
        except Exception as e:
            print("Error in inserting new variable:",e)
    
    def insert_variables_from_raw_formula(self):
        self._raw_formula()
        for formula_desc in self.raw_formula_list:
            self._create_new_column(formula_desc[0])
            self._insert_new_variable(formula_desc)

    def insert_variables_from_python_formula(self,columns):
        self.init_queries()
        rows = self.make_query(columns)
        new_values = []
        create = True
        for row in rows:
            new_variable = self._python_formula(row[0],row[1])
            print(new_variable)
            new_values.append({new_variable[0]:new_variable[1]})
            
        with self.session() as session:
            session.execute(
                self.table.update()
                .where(self.table.c.column_name == new_values[0])  # Adjust column_name to your column name
                .values({self.table.c.column_name: new_values[1]})  # Adjust column_name to your column name
            ) 
            session.commit()
            session.close()

    def aggregate_values_to_helio_step(self, subtable_name='helio_step'):
        # Fetch column names from the source table
        query =f"SELECT column_name FROM information_schema.columns WHERE table_schema='public' AND table_name = '{self.table_name}'" 
        df_columns = pd.read_sql_query(query, self.sql_engine)
        columns = df_columns['column_name'].tolist()

        # Read the entire source table into a DataFrame
        df = pd.read_sql_query(f"SELECT * FROM {self.table_name}", self.sql_engine)

        # Convert the 'Date' column to datetime and truncate it to the nearest minute
        df['Date'] = pd.to_datetime(df['Date']).dt.round('min')

        # Filter rows where the minute component of the 'Date' column is in (0, 15, 30, 45)
        df_filtered = df[df['Date'].dt.minute.isin([0, 15, 30, 45])]

        # Select only the 'Date' and value columns
        df_result = df_filtered[['Date'] + columns[1:]]

        # Sort the result DataFrame by 'Date'
        df_result.sort_values(by='Date', inplace=True)

        # Write the result DataFrame to the subtable
        df_result.to_sql(subtable_name,self.sql_engine, if_exists='replace', index=False)

        # Print the rows of the subtable
        print(df_result)


if __name__ == "__main__":
    manager = None
    try:
        manager = DatabaseSelector()
        manager.connect()

        
        # test for inserting new variables to the table
        # manager.init_import('meteo_data','','')
        # manager.aggregate_values_to_helio_step()
        # input("continue test?")
        manager.init_queries()
        manager.make_query(['wind_speed','Date'])
        input('continue?')
        manager._create_new_column('python_calc')
        manager.insert_variables_from_python_formula(['wind_speed','rel_humidity'])
        # test for the creation of subtables with selected values 
        available_tables = manager.get_available_tables()
        print(f"Available tables in current database '{manager.dbname}' :\n\n{available_tables}\n")
        original_table_name = input("Choose the table name from which you want to select an interval:\n").strip()
        new_table_name = input("Choose the new table name:\n").strip()
        fields = manager.get_fields_in_table(original_table_name)
        print(fields)
        column_name = input("Choose the field to select:\n").strip()
        start = input("Select starting point:\n").strip() 
        end = input("Select ending point:\n").strip()
        manager.select_interval(start, end, column_name, original_table_name, new_table_name)
    except Exception as e:
        print(f"Test failed: {e}")
    finally:
        if manager:
            manager.disconnect()

