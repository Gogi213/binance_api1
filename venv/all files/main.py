# main.py
import pandas as pd
import binance_api
from binance_api import get_binance_data, get_binance_symbols, get_binance_status
from calculate_profit import calculate_profit
from calculate_profit2 import calculate_profit2  # Import new function
from data_processing import custom_format, process_data

data = get_binance_data()
pairs = get_binance_symbols()
status = get_binance_status()
df = calculate_profit(data, pairs)
df['status'] = df['symbol'].map(status)

# Process the data
formatted_df = process_data(df)
formatted_df.to_csv('C:\\Users\\Redmi\\PycharmProjects\\pythonProject1\\venv\\all files\\binance_data.csv', index=False)

df_prices = pd.DataFrame(data)
df_prices['pair'] = df_prices['symbol'].map(pairs)
df_prices['status'] = df_prices['symbol'].map(status)

# Process the prices data
formatted_df_prices = process_data(df_prices)
formatted_df_prices.to_csv('C:\\Users\\Redmi\\PycharmProjects\\pythonProject1\\venv\\all files\\binance_prices.csv', index=False)

print("Data has been written to 'binance_data.csv'")
print("Data has been written to 'binance_prices.csv'")

# Call new function for the second stage of profit calculation
calculate_profit2()

print("Second stage of profit calculation has been completed.")