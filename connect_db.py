import psycopg2
from sqlalchemy import create_engine
import json

def get_url():
    with open('config.json') as fd:
        return json.load(fd)['database']['url']

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

def conn_alchemy_with_url():
    url = get_url()
    try:
        engine = create_engine(url)
        if engine:
            print("[Debug]: Engine created successfully.")
            return engine
        else:
            print("Failed to create engine.")
            exit(1)
    except Exception as e:
        print("Error occurred while creating engine:", e)
        exit(1)


def connect_with_alchemy(dbname,user,password,host,port):
    db_url = f'postgresql://{user}:{password}@{host}:{port}/{dbname}'
    try:
    # Create the SQLAlchemy engine
        engine = create_engine(db_url)
    
    # Check if the engine has been created successfully
        if engine:
            print("[Debug]: Engine created successfully.")
            return engine
        else:
            print("Failed to create engine.")
            exit(1)
    except Exception as e:
        print("Error occurred while creating engine:", e)
        exit(1)

