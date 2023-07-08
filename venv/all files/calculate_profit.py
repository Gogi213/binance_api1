# calculate_profit.py
import pandas as pd

def calculate_profit(data, pairs, usdt=1000, commission=0.001):
    df_data = pd.DataFrame(data)

    # Add a new column with pairs in the format 'BTC/USDT'
    df_data['pair'] = df_data['symbol'].map(pairs)

    # Fill NaN values in the 'pair' column with values from the 'symbol' column
    df_data['pair'].fillna(df_data['symbol'], inplace=True)

    # Split 'pair' column into 'base' and 'quote' columns
    df_data[['base', 'quote']] = df_data['pair'].str.split('/', expand=True)

    # Filter rows where 'quote' is 'USDT'
    df_data = df_data[df_data['quote'] == 'USDT']

    # Convert 'askPrice', 'bidPrice', 'askQty', 'bidQty' to float
    df_data[['askPrice', 'bidPrice', 'askQty', 'bidQty']] = df_data[['askPrice', 'bidPrice', 'askQty', 'bidQty']].astype(float)

    # Calculate buy_amount and usdt_equals
    df_data['buy_amount'] = (usdt / df_data['askPrice']) * (1 - commission)
    df_data['usdt_equals'] = df_data['buy_amount'] * df_data['bidPrice']

    # Calculate profit
    df_data['profit'] = df_data['usdt_equals'] - usdt

    # Add 'swap1' column
    df_data.insert(df_data.columns.get_loc('buy_amount'), 'swap1', df_data['base'])

    # Sort by profit and get top 10
    df_data = df_data.sort_values('profit', ascending=False).head(10)

    return df_data
