import binance_api
from binance_api import get_binance_data, get_binance_symbols
from calculate_profit import calculate_profit

data = get_binance_data()
pairs = get_binance_symbols()
df = calculate_profit(data, pairs)

# Write DataFrame to CSV
df.to_csv('C:\\Users\\Redmi\\PycharmProjects\\pythonProject1\\venv\\all files\\binance_data.csv', index=False)


print("Data has been written to 'binance_data.csv'")
