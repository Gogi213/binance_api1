# calculate_profit.py
import pandas as pd
import numpy as np
from bd import connect_to_db, update_table
from binance_api import get_binance_status

def calculate_profit(data, pairs, usdt=1000, commission=0.001):
    df_data = pd.DataFrame(data)

    # Add a new column with pairs in the format 'BTC/USDT'
    df_data['pair'] = df_data['symbol'].map(pairs)

    # Fill NaN values in the 'pair' column with values from the 'symbol' column
    df_data['pair'].fillna(df_data['symbol'], inplace=True)

    # Split 'pair' column into 'base' and 'quote' columns
    df_data[['base', 'quote']] = df_data['pair'].str.split('/', expand=True)

    # Convert 'askPrice', 'bidPrice', 'askQty', 'bidQty' to float
    df_data[['askprice', 'bidprice', 'askqty', 'bidqty']] = df_data[['askprice', 'bidprice', 'askqty', 'bidqty']].astype(float)

    # Filter rows with zero values in 'askPrice', 'bidPrice', 'askQty', 'bidQty'
    df_data = df_data[(df_data['askprice'] != 0) & (df_data['bidprice'] != 0) & (df_data['askqty'] != 0) & (df_data['bidqty'] != 0)]

    # Create two dataframes for buy and sell operations
    df_buy = df_data[df_data['quote'] == 'USDT'].copy()
    df_sell = df_data[df_data['base'] == 'USDT'].copy()

    # Calculate buy_amount and usdt_equals for buy operations
    df_buy['amount'] = (usdt / df_buy['askprice']) * (1 - commission)
    df_buy['usdt_equals'] = df_buy['amount'] * (df_buy['bidprice'] + df_buy['askprice'])/2

    # Calculate sell_amount and usdt_equals for sell operations
    df_sell['amount'] = (usdt * (1 - commission)) / df_sell['bidprice']
    df_sell['usdt_equals'] = df_sell['amount'] * (df_sell['bidprice'] + df_sell['askprice'])/2

    # Calculate profit for both operations
    df_buy['profit'] = (df_buy['usdt_equals'] - usdt) / usdt * 100
    df_sell['profit'] = (df_sell['usdt_equals'] - usdt) / usdt * 100

    # Add 'swap' column
    df_buy.insert(df_buy.columns.get_loc('amount'), 'swap', df_buy['base'])
    df_sell.insert(df_sell.columns.get_loc('amount'), 'swap', df_sell['quote'])


    df_buy.to_csv('C:\\Users\\Redmi\\PycharmProjects\\pythonProject1\\venv\\all files\\debug_buy.csv')
    df_sell.to_csv('C:\\Users\\Redmi\\PycharmProjects\\pythonProject1\\venv\\all files\\debug_sell.csv')

    # Concatenate buy and sell dataframes
    df_data = pd.concat([df_buy, df_sell])

    # Add placeholders for new columns
    new_columns = ['pair2', 'base2', 'quote2', 'bidprice2', 'bidqty2', 'askprice2', 'askqty2', 'swap2', 'amount2', 'usdt_equals2', 'profit2','pair3', 'base3', 'quote3', 'bidprice3', 'bidqty3', 'askprice3', 'askqty3', 'swap3', 'amount3', 'usdt_equals3', 'profit3']
    for col in new_columns:
        df_data[col] = np.nan

    # Rearrange columns
    df_data = df_data[['symbol', 'pair', 'base', 'quote', 'bidprice', 'bidqty', 'askprice', 'askqty', 'swap', 'amount', 'usdt_equals', 'profit'] + new_columns]

    # Sort by profit and get top 10
    df_data = df_data.sort_values('profit', ascending=False).head(5000)

    # Добавьте следующую строку перед вызовом update_table
    df_data['status'] = df_data['symbol'].map(get_binance_status())

    # Получите список имен столбцов
    cols = list(df_data.columns)

    # Переместите столбец 'status' сразу после 'symbol'
    cols.insert(1, cols.pop(cols.index('status')))

    # Переиндексируйте DataFrame
    df_data = df_data.reindex(columns=cols)
    conn = connect_to_db()

    update_table(conn, df_data, 'binance_data')
    return df_data
