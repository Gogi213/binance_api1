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

def update_table(conn, df, table_name):
    cursor = conn.cursor()
    # Delete all rows from the table
    cursor.execute(f'DELETE FROM {table_name}')
    # Insert new rows into the table
    for index, row in df.iterrows():
        cursor.execute(
            f"""INSERT INTO {table_name} (symbol, status, pair, base, quote, bidprice, bidqty, askprice, askqty, 
                                          swap, amount, usdt_equals, profit, 
                                          pair2, base2, quote2, bidprice2, bidqty2, askprice2, askqty2, 
                                          swap2, amount2, usdt_equals2, profit2)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
            (row['symbol'], row['status'], row['pair'], row['base'], row['quote'], row['bidprice'], row['bidqty'],
             row['askprice'], row['askqty'], row['swap'], row['amount'], row['usdt_equals'], row['profit'],
             row['pair2'], row['base2'], row['quote2'], row['bidprice2'], row['bidqty2'], row['askprice2'],
             row['askqty2'],
             row['swap2'], row['amount2'], row['usdt_equals2'], row['profit2'])
        )
    conn.commit()
    cursor.close()