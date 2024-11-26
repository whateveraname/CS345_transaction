import psycopg2
import threading
import time
import os
import subprocess
import sys

config = {
    'host': 'localhost',
    'port': 5432, 
    'user': 'postgres', 
    'password': 'postgres',
    'dbname': 'practicedb'
}

def import_datasets():
    config['dbname'] = 'postgres'
    conn = psycopg2.connect(**config)
    conn.autocommit = True
    cursor = conn.cursor()
    cursor.execute(f"DROP DATABASE IF EXISTS practicedb;")
    cursor.execute(f"CREATE DATABASE practicedb OWNER {config['user']};")
    cursor.execute(f"DROP DATABASE IF EXISTS imdb2015;")
    cursor.execute(f"CREATE DATABASE imdb2015 OWNER {config['user']};")
    cursor.execute(f"DROP DATABASE IF EXISTS customer;")
    cursor.execute(f"CREATE DATABASE customer OWNER {config['user']};")
    cursor.execute(f"DROP ROLE IF EXISTS david;")
    cursor.execute(f"CREATE ROLE david;")
    conn.close()

    print('Importing practicedb dataset:')
    config['dbname'] = 'practicedb'
    conn = psycopg2.connect(**config)
    with open('/content/drive/MyDrive/transaction/practiceData.sql', 'r') as f:
        commands = f.read()
        cursor = conn.cursor()
        cursor.execute(commands)
        cursor.close()
        conn.commit()
    print('practicedb imported successfully.')
    conn.close()

    print('Importing imdb dataset:')
    config['dbname'] = 'imdb2015'
    os.system('psql postgresql://postgres:postgres@localhost:5432/imdb2015 -f /content/drive/MyDrive/transaction/create_imdb2015.sql')
    # conn = psycopg2.connect(**config)
    # cursor = conn.cursor()
    # cursor.execute('\i /content/drive/MyDrive/transaction/create_imdb2015.sql')
    # with open('/content/drive/MyDrive/transaction/create_imdb2015.sql', 'r') as f:
    #     commands = f.read()
    #     cursor = conn.cursor()
    #     cursor.execute(commands)
    #     cursor.close()
    #     conn.commit()
    print('imdb imported successfully.')
    # conn.close()

    print('Importing customer dataset:')
    config['dbname'] = 'customer'
    conn = psycopg2.connect(**config)
    with open('/content/drive/MyDrive/transaction/setup.sql', 'r') as f:
        commands = f.read()
        cursor = conn.cursor()
        cursor.execute(commands)
        cursor.close()
        conn.commit()
    print('customer imported successfully.')
    conn.close()

    config['dbname'] = 'practicedb'
