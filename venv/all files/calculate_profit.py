import pandas as pd
import numpy as np

def calculate_profit(data, pairs, usdt=1000, commission=0.001):
    df_data = pd.DataFrame(data)

    # Add a new column with pairs in the format 'BTC/USDT'
    df_data['pair'] = df_data['symbol'].map(pairs)

    # Fill NaN values in the 'pair' column with values from the 'symbol' column
    df_data['pair'].fillna(df_data['symbol'], inplace=True)

    # Split 'pair' column into 'base' and 'quote' columns
    df_data[['base', 'quote']] = df_data['pair'].str.split('/', expand=True)

    # Convert 'askPrice', 'bidPrice', 'askQty', 'bidQty' to float
    df_data[['askPrice', 'bidPrice', 'askQty', 'bidQty']] = df_data[['askPrice', 'bidPrice', 'askQty', 'bidQty']].astype(float)

    # Filter rows with zero values in 'askPrice', 'bidPrice', 'askQty', 'bidQty'
    df_data = df_data[(df_data['askPrice'] != 0) & (df_data['bidPrice'] != 0) & (df_data['askQty'] != 0) & (df_data['bidQty'] != 0)]

    # Create two dataframes for buy and sell operations
    df_buy = df_data[df_data['quote'] == 'USDT'].copy()
    df_sell = df_data[df_data['base'] == 'USDT'].copy()

    # Calculate buy_amount and usdt_equals for buy operations
    df_buy['amount'] = (usdt / df_buy['askPrice']) * (1 - commission)
    df_buy['usdt_equals'] = df_buy['amount'] * df_buy['bidPrice']

    # Calculate sell_amount and usdt_equals for sell operations
    df_sell['amount'] = (usdt * (1 - commission)) / df_sell['bidPrice']
    df_sell['usdt_equals'] = df_sell['amount'] * df_sell['askPrice']

    # Calculate profit for both operations
    df_buy['profit'] = (df_buy['usdt_equals'] - usdt) / usdt * 100
    df_sell['profit'] = (usdt - df_sell['usdt_equals']) / usdt * 100

    # Add 'swap' column
    df_buy.insert(df_buy.columns.get_loc('amount'), 'swap', df_buy['base'])
    df_sell.insert(df_sell.columns.get_loc('amount'), 'swap', df_sell['quote'])

    # Concatenate buy and sell dataframes
    df_data = pd.concat([df_buy, df_sell])

    # Add placeholders for new columns
    new_columns = ['pair2', 'base2', 'quote2', 'bidPrice2', 'bidQty2', 'askPrice2', 'askQty2', 'swap2', 'amount2', 'usdt_equals2', 'profit2']
    for col in new_columns:
        df_data[col] = np.nan

    # Rearrange columns
    df_data = df_data[['symbol', 'pair', 'base', 'quote', 'bidPrice', 'bidQty', 'askPrice', 'askQty', 'swap', 'amount', 'usdt_equals', 'profit'] + new_columns]

    # Sort by profit and get top 10
    df_data = df_data.sort_values('profit', ascending=False).head(900)

    return df_data