# bd.py
import psycopg2
from psycopg2 import sql
import pandas as pd

def connect_to_db(database='binance_project', user='postgres', password='19938713', host='localhost', port='5432'):
    conn = psycopg2.connect(database=database, user=user, password=password, host=host, port=port)
    return conn

def close_connection(conn):
    conn.close()

def insert_data_to_table(conn, df, table_name):
    cursor = conn.cursor()

    # Create the insert query
    query = sql.SQL('INSERT INTO {} ({}) VALUES ({})').format(
        sql.Identifier(table_name),
        sql.SQL(',').join(map(sql.Identifier, ['symbol', 'status', 'bidPrice', 'bidQty', 'askPrice', 'askQty', 'pair', 'base', 'quote'])),
        sql.SQL(',').join(sql.Placeholder() * 9)
    )

    # Insert the data into the table
    for index, row in df.iterrows():
        values = (row['symbol'], row['status'], row['bidprice'], row['bidqty'], row['askprice'], row['askqty'], row['pair'], row['base'], row['quote'])
        cursor.execute(query, values)

    # Commit the changes
    conn.commit()

    # Close the cursor
    cursor.close()