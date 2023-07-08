# main.py
import pandas as pd
import binance_api
from binance_api import get_binance_data, get_binance_symbols
from calculate_profit import calculate_profit

data = get_binance_data()
pairs = get_binance_symbols()
df = calculate_profit(data, pairs)

# Write DataFrame to CSV
df.to_csv('C:\\Users\\Redmi\\PycharmProjects\\pythonProject1\\venv\\all files\\binance_data.csv', index=False)

# Create a new DataFrame for binance prices
df_prices = pd.DataFrame(data)

# Add a new column with pairs in the format 'BTC/USDT'
df_prices['pair'] = df_prices['symbol'].map(pairs)

# Fill NaN values in the 'pair' column with values from the 'symbol' column
df_prices['pair'].fillna(df_prices['symbol'], inplace=True)

# Split 'pair' column into 'base' and 'quote' columns
df_prices[['base', 'quote']] = df_prices['pair'].str.split('/', expand=True)

# Convert 'askPrice', 'bidPrice', 'askQty', 'bidQty' to float
df_prices[['askPrice', 'bidPrice', 'askQty', 'bidQty']] = df_prices[['askPrice', 'bidPrice', 'askQty', 'bidQty']].astype(float)

# Filter out rows where 'bidPrice' or 'askPrice' are less than or equal to 0
df_prices = df_prices.loc[(df_prices['bidPrice'] > 0) & (df_prices['askPrice'] > 0)]

# Write DataFrame to CSV
df_prices.to_csv('C:\\Users\\Redmi\\PycharmProjects\\pythonProject1\\venv\\all files\\binance_prices.csv', index=False)

print("Data has been written to 'binance_data.csv'")
print("Data has been written to 'binance_prices.csv'")