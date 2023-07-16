# bd.py
from sqlalchemy import create_engine
import pandas as pd
import psycopg2

def connect_to_db(database='binance_project', user='postgres', password='19938713', host='localhost', port='5432'):
    conn = psycopg2.connect(database=database, user=user, password=password, host=host, port=port)
    return conn

def update_table(conn, df, table_name):
    cursor = conn.cursor()
    cursor.execute(f'DELETE FROM {table_name}')
    for index, row in df.iterrows():
        if 'swap' in df.columns:
            cursor.execute(
                f"""INSERT INTO {table_name} (symbol, status, pair, base, quote, bidprice, bidqty, askprice, askqty, swap, amount, usdt_equals, profit, pair2, base2, quote2, bidprice2, bidqty2, askprice2, askqty2, swap2, amount2, usdt_equals2, profit2, pair3, base3, quote3, bidprice3, bidqty3, askprice3, askqty3, swap3, amount3, usdt_equals3, profit3) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                (row['symbol'], row['status'], row['pair'], row['base'], row['quote'], row['bidprice'], row['bidqty'], row['askprice'], row['askqty'], row['swap'], row['amount'], row['usdt_equals'], row['profit'], row['pair2'], row['base2'], row['quote2'], row['bidprice2'], row['bidqty2'], row['askprice2'], row['askqty2'], row['swap2'], row['amount2'], row['usdt_equals2'], row['profit2'], row['pair3'], row['base3'], row['quote3'], row['bidprice3'], row['bidqty3'], row['askprice3'], row['askqty3'], row['swap3'], row['amount3'], row['usdt_equals3'], row['profit3'])
            )
        else:
            cursor.execute(
                f"""INSERT INTO {table_name} (symbol, status, pair, base, quote, bidprice, bidqty, askprice, askqty) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                (row['symbol'], row['status'], row['pair'], row['base'], row['quote'], row['bidprice'], row['bidqty'], row['askprice'], row['askqty'])
            )
    conn.commit()
    cursor.close()