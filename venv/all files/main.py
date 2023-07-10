# main.py
import pandas as pd
import binance_api
from binance_api import get_binance_data, get_binance_symbols
from calculate_profit import calculate_profit
from calculate_profit2 import calculate_profit2  # Import new function

def custom_format(x):
    if isinstance(x, float):
        str_x = format(x, '.15f')  # Convert number to fixed-point notation
        if '.' in str_x:
            decimal_part = str_x.split('.')[1]
            num_decimals = len(decimal_part.rstrip('0'))  # Remove trailing zeros
            return format(x, f'.{num_decimals}f')
    return x

data = get_binance_data()
pairs = get_binance_symbols()
df = calculate_profit(data, pairs)

formatted_df = df.applymap(custom_format)
formatted_df.to_csv('C:\\Users\\Redmi\\PycharmProjects\\pythonProject1\\venv\\all files\\binance_data.csv', index=False)

df_prices = pd.DataFrame(data)
df_prices['pair'] = df_prices['symbol'].map(pairs)
df_prices['pair'].fillna(df_prices['symbol'], inplace=True)
df_prices[['base', 'quote']] = df_prices['pair'].str.split('/', expand=True)
df_prices[['askPrice', 'bidPrice', 'askQty', 'bidQty']] = df_prices[['askPrice', 'bidPrice', 'askQty', 'bidQty']].astype(float)
df_prices = df_prices.loc[(df_prices['bidPrice'] > 0) & (df_prices['askPrice'] > 0)]

formatted_df_prices = df_prices.applymap(custom_format)
formatted_df_prices.to_csv('C:\\Users\\Redmi\\PycharmProjects\\pythonProject1\\venv\\all files\\binance_prices.csv', index=False)

print("Data has been written to 'binance_data.csv'")
print("Data has been written to 'binance_prices.csv'")

# Call new function for the second stage of profit calculation
calculate_profit2()

print("Second stage of profit calculation has been completed.")