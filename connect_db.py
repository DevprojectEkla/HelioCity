import psycopg2
import json


def open_config():
    with open('config.json') as fd:
        config = json.load(fd)
        config_db = config['database']
        dbname = config_db['dbname']
        user = config_db["user"]
        password = config_db["password"] 
        host = config_db["host"]
        port = config_db["port"]
        fd.close()
    return dbname, user, password, host, port


def connect_to_db(dbname,user,password,host,port):
    conn = None
    cursor = None
    try :
        conn = psycopg2.connect(
                dbname=dbname,
                user=user,
                password=password,
                host=host,
                port=port
                )

        cursor = conn.cursor()
        return conn, cursor

    except psycopg2.Error as e:
    
        print("Error connecting to database",e)
        exit(1)



