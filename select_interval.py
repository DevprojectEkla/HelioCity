from connect_db import connect_to_db, open_config 
from utils import confirm_and_commit, print_rows, close_conn




def select_interval(start,end, column_name, source_table_name,new_table_name):

    dbname, user, password, host, port = open_config()
    conn, cursor = connect_to_db(dbname,user, password, host, port)
    select_query = f"""
    SELECT * INTO {new_table_name}
    FROM {source_table_name}
    WHERE {column_name} >= %s AND {column_name} <= %s
    """

    cursor.execute(select_query,(start,end))

    print_rows(new_table_name,cursor)
    confirm_and_commit(conn)
    close_conn(conn,cursor)

if __name__ == "__main__":
    original_table_name = input("choose the table name from which you want to select an interval").strip()
    new_table_name = input("choose the new table name").strip()
    column_name = input("choose the field to select").strip()

    start = input("select starting point:").strip() 
    end = input("select ending point:").strip()

    select_interval(start,end,column_name, original_table_name,new_table_name)
