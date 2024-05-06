def print_rows(table_name,cursor):
    cursor.execute(f"SELECT * FROM {table_name}")
    rows = cursor.fetchall()
    if rows:
        for row in rows:
            print(row)

    else:

        print('the table is empty')

def confirm_and_commit(conn):
    commit = input("Do you want to commit? 'y or n'")

    if commit.strip() == 'y':
        conn.commit()

    else:

        print("aborting")
        exit(1)

def close_conn(conn,cursor):
    if conn:
        if cursor:
            cursor.close()
            print("cursor successfully disconnected")
        conn.close()
        print("DB Connection closed")



