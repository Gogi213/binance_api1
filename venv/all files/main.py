# main.py
import pandas as pd
import binance_api


from binance_api import get_binance_data, get_binance_symbols, get_binance_status
from calculate_profit import calculate_profit
from calculate_profit2 import calculate_profit2
from calculate_profit3 import calculate_profit3
from data_processing import custom_format, process_data
from bd import connect_to_db, update_table

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

engine = connect_to_db()

update_table(engine, formatted_df_prices, 'binance_prices')
update_table(engine, formatted_df, 'binance_data')

calculate_profit2()
print("Second stage of profit calculation has been completed.")

calculate_profit3()
print("Third stage of profit calculation has been completed.")