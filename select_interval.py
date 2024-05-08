from connect_db import conn_alchemy_with_url, connect_with_alchemy, open_config 
from utils import get_available_tables, print_rows

def select_interval(start, end, column_name, source_table_name, new_table_name, engine):
    try:
        select_query = f"""
            SELECT * INTO {new_table_name}
            FROM {source_table_name}
            WHERE "{column_name}" >= %s AND "{column_name}" <= %s
        """
        with engine.connect() as conn:
            conn.execute(select_query, (start, end))
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    dbname, _, _, _, _ = open_config()
    engine = None
    try:
        engine = conn_alchemy_with_url()
        available_tables = get_available_tables(engine)
        print(f"Available tables in current database '{dbname}' :\n\n{available_tables}\n")
        original_table_name = input("Choose the table name from which you want to select an interval:\n").strip()
        new_table_name = input("Choose the new table name:\n").strip()
        column_name = input("Choose the field to select:\n").strip()
        start = input("Select starting point:\n").strip() 
        end = input("Select ending point:\n").strip()
        select_interval(start, end, column_name, original_table_name, new_table_name, engine)
    except Exception as e:
        print(f"Error: {e}")
    finally:
        if engine is not None:
            engine.dispose()

