from sqlalchemy import text
import pandas as pd
import math

from database_handler import DatabaseHandler

class DatabaseSelector(DatabaseHandler):
    """ DataBaseSelector herits from DatabaseHandler. It is the main class
    to manipulate data and perform simple operations on tables like selecting
    an interval of data, performing a calculation on values of a table and add
    the new column containing the newly calculated values """

    def __init__(self):
        super().__init__()
        self.raw_formula_list = None 
        self.insert_column_name = None

    @classmethod 
    def new(cls,table_name,csv_file_path,flag):
        instance = cls() 
        instance.connect()
        instance.init_import(table_name,csv_file_path,flag)
        return instance 

    def select_interval(self, start, end, column_name, new_table_name):
        # select an interval of values and create a subtable containing only
        # the selected values.
        # The selection criteria consists in a starting point and an ending point
        # e.g for a Date column: start=2023-12-01 end=2023-12-31
        try:
            select_query = f"""
                DROP TABLE IF EXISTS {new_table_name};
                SELECT * INTO {new_table_name}
                FROM {self.table_name}
                WHERE "{column_name}" >= '{start}'
                AND "{column_name}" <= '{end}'
            """
            
            with self.sql_engine.connect() as conn:
                conn.execute(text(select_query))
                conn.commit()
                print(
                        f"Interval selected successfully and stored in table"
                        f"' {new_table_name}'.")
        except Exception as e:
            print(f"Error in query: {e}")

    def order_table_by(self, column_name, new_table_name):
    # Order the table by the specified column name and create a new table
        try:
            select_query = f"""
                DROP TABLE IF EXISTS {new_table_name};
                SELECT * INTO {new_table_name}
                FROM {self.table_name}
                ORDER BY "{column_name}"
            """
            
            with self.sql_engine.connect() as conn:
                conn.execute(text(select_query))
                conn.commit()
                print(
                    f"Table ordered successfully by column '{column_name}' and stored in table '{new_table_name}'."
                )
        except Exception as e:
            print(f"Error in query: {e}")
    
    def select_scope(self, column_name, scope, new_table_name):
    # Select a scope of values and create a new table with it
        try:
            select_query = f"""
                DROP TABLE IF EXISTS {new_table_name};
                SELECT * INTO {new_table_name}
                FROM {self.table_name}
                WHERE "{column_name}" = '{scope}'
            """
            
            with self.sql_engine.connect() as conn:
                conn.execute(text(select_query))
                conn.commit()
                print(
                    f"Table selected successfully by column '{column_name}' and scope '{scope}';\n"
                    f"Successfully inserted in table '{new_table_name}'."
                )
        except Exception as e:
            print(f"Error in query: {e}")

    def _raw_formula(self):
        # Private method, should not be used directly.
        # This is an example of formula that we can feed directly in SQL
        # to calculate a new variable.
        # It is hardcoded here to calculate the apparent temperature based on
        # meteo data. 
        # This feeds the public method insert_variables_from_raw_formula
        self.insert_column_name = 'Apparent_temp'
        sql_code = """
        CAST((-2.7 * (1 - (rel_humidity / 100))
            * (5.0 * SQRT(GREATEST(wind_speed, 0)) - 0.1)
            + "Temperature") AS numeric(10,2))
        """
        self.raw_formula_list = [(f"{self.insert_column_name}",sql_code)]

    def _python_formula(self,temperature,wind_speed,rel_humidity):
        # Private method, should not be use directly
        # Here we can use python modules and functions to perform more advanced
        # calculations, this will feed the public method  
        # insert_variables_from_python_formula() to perform calculation on each
        # row.
        if rel_humidity and wind_speed and temperature:
            result = -2.7 * (1 - (rel_humidity / 100)) * (5.0 * math.sqrt(max(wind_speed, 0)) - 0.1) + temperature
            value = round(result,2)
        else:
            # WARNING: this is to test the helio_example remove this line and
            # uncomment the other for safety
            value = temperature + wind_speed + rel_humidity
            #value = None
        return value

    def _create_new_column(self,column_name):
        # Private method, should not be use directly

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
        # Private method, should not be use directly
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
        # This is the public method to create the column of a new variable
        # using raw SQL formula
        # perform calculation and add the column to the selected table
        self._raw_formula()
        for formula_desc in self.raw_formula_list:
            self._create_new_column(formula_desc[0])
            self._insert_new_variable(formula_desc)

    def insert_variables_from_python_formula(self,columns):
        # This is the public method to create the column of a new variable
        # perform calculation with Python modules or advanced features
        # and add the column to the selected table
        self.insert_column_name = 'python_calc'

        df = pd.read_sql_table(self.table_name, self.sql_engine)
        values = []
        for _,row in df.iterrows():
            value = self._python_formula(row[columns[0]],# Temperature
                                         row[columns[1]],# wind_speed
                                         row[columns[2]])# rel_humidity
            values += [value]
        df[self.insert_column_name] = values
        print(f""":: Inserting new column ::\n
              column_name:{self.insert_column_name}\n
              values: {values[:100]}\n[...]\n{values[-100:]}""")

        df.to_sql(self.table_name,
                  self.sql_engine,
                  if_exists='replace',
                  index=False)

    def aggregate_values_to_helio_step(self, subtable_name='helio_step'):
        # This method helps aggregating values of a table
        # containing a row of timestamps to filter data for a step of 15 min

        # Fetch column names from the source table
        query =f"""SELECT column_name FROM information_schema.columns
        WHERE table_schema='public'
        AND table_name = '{self.table_name}'""" 
        # Launch the query with pandas and SQL_alchemy engine
        df_columns = pd.read_sql_query(query, self.sql_engine)
        columns = df_columns['column_name'].tolist()

        # Read the entire source table into a DataFrame
        df = pd.read_sql_query(f"SELECT * FROM {self.table_name}",
                               self.sql_engine)

        # Convert the 'Date' column to datetime and truncate it
        # to the nearest minute
        df['Date'] = pd.to_datetime(df['Date']).dt.round('min')

        # Filter rows where the minute component of the 'Date' column
        # is in (0, 15, 30, 45)
        df_filtered = df[df['Date'].dt.minute.isin([0, 15, 30, 45])]

        # Select only the 'Date' and value columns
        df_result = df_filtered[['Date'] + columns[1:]]

        # Sort the result DataFrame by 'Date'
        df_result.sort_values(by='Date', inplace=True)

        # Write the result DataFrame to the subtable
        df_result.to_sql(subtable_name,self.sql_engine,
                         if_exists='replace', index=False)

        # Print the rows of the subtable
        print(df_result)


if __name__ == "__main__":
    # test de la classe et de ses principales méthodes
    manager = None
    try:
        manager = DatabaseSelector()
        manager.connect()
        # test for inserting new variables to the table
        manager.init_import('meteo_data','','')
        # manager.aggregate_values_to_helio_step()
        # input("continue test?")
        manager.init_queries()
        manager.make_query(['wind_speed','Date'])
        input('continue?')
        manager.insert_variables_from_python_formula(['wind_speed','rel_humidity'])
        # test for the creation of subtables with selected values 
        available_tables = manager.get_available_tables()
        print(f"Available tables in current database '{manager.dbname}' :\n\n{available_tables}\n")
        original_table_name = input(
                "Choose the table name from which you want to select an interval:\n").strip()
        new_table_name = input("Choose the new table name:\n").strip()
        fields = manager.get_fields_in_table(original_table_name)
        print(fields)
        column_name = input("Choose the field to select:\n").strip()
        start = input("Select starting point:\n").strip() 
        end = input("Select ending point:\n").strip()
        manager.select_interval(start, end,
                                column_name, original_table_name,
                                new_table_name)
    except Exception as e:
        print(f"Test failed: {e}")
    finally:
        if manager:
            manager.disconnect()

