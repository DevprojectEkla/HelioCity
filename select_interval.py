from connect_db import connect_to_db, connect_with_alchemy, open_config 
from utils import confirm_and_commit, get_available_tables, print_rows, close_conn




def select_interval(start,end, column_name, source_table_name,new_table_name):

    dbname, user, password, host, port = open_config()
    conn, cursor = connect_to_db(dbname,user, password, host, port)
    select_query = f"""
    SELECT * INTO {new_table_name}
    FROM {source_table_name}
    WHERE "{column_name}" >= %s AND "{column_name}" <= %s
    """

    cursor.execute(select_query,(start,end))

    print_rows(new_table_name,cursor)
    confirm_and_commit(conn)
    close_conn(conn,cursor)

if __name__ == "__main__":
    dbname, user, password, host, port = open_config()
    engine = connect_with_alchemy(dbname,user,password,host,port)
    available_tables = get_available_tables(engine)
    print(f"Available tables in current database '{dbname}' :\n\n{available_tables}\n")
    original_table_name = input("choose the table name from which you want to select an interval:\n").strip()
    new_table_name = input("choose the new table name\n").strip()
    column_name = input("choose the field to select\n").strip()

    start = input("select starting point:").strip() 
    end = input("select ending point:").strip()

    select_interval(start,end,column_name, original_table_name,new_table_name)
    engine.dispose()
