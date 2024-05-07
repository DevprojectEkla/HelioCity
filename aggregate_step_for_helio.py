import pandas as pd
from connect_db import connect_to_db, connect_with_alchemy, open_config
from utils import confirm_and_commit, get_available_tables, print_rows

def input_source(engine):
    default_table_name = 'meteo_data'
    tables_list = get_available_tables(engine)
    print("Available tables in database:", tables_list)
    table_name = input(f"select an existing table from API to size for the calculator's step: "
                   f"(default:{default_table_name})") or default_table_name
    return table_name

def create_subtable_step_helio(engine, source_table_name, subtable_name='helio_step'):
    # Fetch column names from the source table
    df_columns = pd.read_sql_query(f"SELECT column_name FROM information_schema.columns WHERE table_name = '{source_table_name}'", engine)
    columns = df_columns['column_name'].tolist()

    # Grab the column names for the SELECT query, excluding the first column ('Date')
    value_columns = ", ".join(columns[1:])

    # Read the entire source table into a DataFrame
    df = pd.read_sql_query(f"SELECT * FROM {source_table_name}", engine)

    # Convert the 'Date' column to datetime and truncate it to the nearest minute
    df['Date'] = pd.to_datetime(df['Date']).dt.round('min')

    # Filter rows where the minute component of the 'Date' column is in (0, 15, 30, 45)
    df_filtered = df[df['Date'].dt.minute.isin([0, 15, 30, 45])]

    # Select only the 'Date' and value columns
    df_result = df_filtered[['Date'] + columns[1:]]

    # Sort the result DataFrame by 'Date'
    df_result.sort_values(by='Date', inplace=True)

    # Write the result DataFrame to the subtable
    df_result.to_sql(subtable_name, engine, if_exists='replace', index=False)

    # Print the rows of the subtable
    print(df_result)


# Example usage:
# create_subtable_step_helio(engine, 'your_source_table_name')


if __name__ == '__main__':

    dbname, user, password, host, port = open_config()
    engine = connect_with_alchemy(dbname, user, password, host, port)
    table_name = input_source(engine)
    create_subtable_step_helio(engine, table_name)
    engine.dispose()
