import requests

def get_binance_data():
    url = 'https://api.binance.com/api/v3/ticker/bookTicker'
    response = requests.get(url)
    data = response.json()
    return data

def get_binance_symbols():
    url = 'https://api.binance.com/api/v3/exchangeInfo'
    response = requests.get(url)
    data = response.json()
    symbols = data['symbols']
    pairs = {f"{symbol['baseAsset']}{symbol['quoteAsset']}": f"{symbol['baseAsset']}/{symbol['quoteAsset']}" for symbol in symbols}
    return pairs
