from connect_db import open_config,conn_alchemy_with_url
import pandas as pd

class DatabaseManager:
    def __init__(self):
        self.engine = None
        self.dbname, _, _, _, _ = open_config()

    def connect(self):
        try:
            self.engine = conn_alchemy_with_url()
            print("Connected to the database successfully!")
        except Exception as e:
            print(f"Error: {e}")

    def disconnect(self):
        if self.engine is not None:
            self.engine.dispose()
            print("Disconnected from the database.")

    def select_interval(self, start, end, column_name, source_table_name, new_table_name):
        try:
            select_query = f"""
                SELECT * INTO {new_table_name}
                FROM {source_table_name}
                WHERE "{column_name}" >= %s AND "{column_name}" <= %s
            """
            with self.engine.connect() as conn:
                conn.execute(select_query, (start, end))
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
            df_tables = pd.read_sql_query(query, self.engine)
            return df_tables['table_name'].tolist()
        except Exception as e:
            print(f"Error fetching available tables: {e}")

    def get_fields_in_table(self):
        try:
            query = """
            SELECT column_name FROM information_schema.column_name

            """

if __name__ == "__main__":
    manager = None
    try:
        manager = DatabaseManager()
        manager.connect()
        available_tables = manager.get_available_tables()
        print(f"Available tables in current database '{manager.dbname}' :\n\n{available_tables}\n")
        original_table_name = input("Choose the table name from which you want to select an interval:\n").strip()
        new_table_name = input("Choose the new table name:\n").strip()
        column_name = input("Choose the field to select:\n").strip()
        start = input("Select starting point:\n").strip() 
        end = input("Select ending point:\n").strip()
        manager.select_interval(start, end, column_name, original_table_name, new_table_name)
    except Exception as e:
        print(f"Error: {e}")
    finally:
        if manager:
            manager.disconnect()

