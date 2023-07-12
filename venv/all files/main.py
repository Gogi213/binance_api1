# main.py
import pandas as pd
import binance_api
from binance_api import get_binance_data, get_binance_symbols, get_binance_status
from calculate_profit import calculate_profit
from calculate_profit2 import calculate_profit2 # Import new function
from data_processing import custom_format, process_data
import psycopg2 # Import psycopg2 library

# Establish a connection to the database
def connect_to_db(database='binance_project', user='postgres', password='19938713', host='localhost', port='5432'):
    conn = psycopg2.connect(database=database, user=user, password=password, host=host, port=port)
    return conn

# Function to update a table in the database
def update_table(conn, df, table_name):
    cursor = conn.cursor()
    # Delete existing data from the table
    cursor.execute(f'DELETE FROM {table_name}')
    # Insert new data into the table
    for index, row in df.iterrows():
        cursor.execute(
            f"""INSERT INTO {table_name} (symbol, status, bidprice, bidqty, askprice, askqty, pair, base, quote) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)""",
            (row['symbol'], row['status'], row['bidprice'], row['bidqty'], row['askprice'], row['askqty'], row['pair'], row['base'], row['quote'])
        )
    # Commit the changes and close the cursor
    conn.commit()
    cursor.close()

data = get_binance_data()
df_data = pd.DataFrame(data)
df_data = df_data.rename(columns={'bidPrice': 'bidprice', 'bidQty': 'bidqty', 'askPrice': 'askprice', 'askQty': 'askqty'})
pairs = get_binance_symbols()
status = get_binance_status()
df = calculate_profit(df_data, pairs)
df['status'] = df['symbol'].map(status)
pairs = get_binance_symbols()
status = get_binance_status()
df = calculate_profit(data, pairs)
df['status'] = df['symbol'].map(status)
df = df.rename(columns={'bidPrice': 'bidprice', 'bidQty': 'bidqty', 'askPrice': 'askprice', 'askQty': 'askqty'})
formatted_df = process_data(df)

df_prices = pd.DataFrame(data)
df_prices['pair'] = df_prices['symbol'].map(pairs)
df_prices = df_prices.rename(columns={'bidPrice': 'bidprice', 'bidQty': 'bidqty', 'askPrice': 'askprice', 'askQty': 'askqty'})
df_prices['status'] = df_prices['symbol'].map(status)
formatted_df_prices = process_data(df_prices)

# Connect to the database
conn = connect_to_db()

# Update the 'binance_prices' and 'binance_data' tables
update_table(conn, formatted_df_prices, 'binance_prices')
update_table(conn, formatted_df, 'binance_data')

# Close the connection to the database
conn.close()

print("Data has been written to 'binance_data.csv'")
print("Data has been written to 'binance_prices.csv'")

# Call new function for the second stage of profit calculation
calculate_profit2()
print("Second stage of profit calculation has been completed.")