from connect_db import connect_to_db, open_config
from utils import confirm_and_commit, input_source, print_rows


def create_subtable_step_helio(cursor, conn, source_table_name, subtable_name='helio_step'):
#grab the values of columns after the TIMESTAMP
    cursor.execute(f"SELECT column_name FROM information_schema.columns WHERE table_name = '{source_table_name}'")
    columns = [row[0] for row in cursor.fetchall()]
    value_columns = ", ".join(columns[1:])

    create_subtable_query= f"""
    SELECT 
        date_trunc('minute', date) AS start_interval,
        {value_columns}
    INTO
        {subtable_name}
    FROM
        {source_table_name}
    WHERE
        EXTRACT(MINUTE FROM date) in  (0, 15 , 30, 45)
    ORDER BY
        start_interval
    """
    cursor.execute(create_subtable_query)
    print_rows(subtable_name,cursor)
    confirm_and_commit(conn)

if __name__ == '__main__':

    dbname, user, password, host, port = open_config()
    conn, cursor = connect_to_db(dbname, user, password, host, port)
    table_name, _, _ = input_source()
    create_subtable_step_helio(cursor, conn, table_name)
