import pandas as pd
import numpy as np

def calculate_profit2():
    # Load data from csv files
    df_data = pd.read_csv('C:\\Users\\Redmi\\PycharmProjects\\pythonProject1\\venv\\all files\\binance_data.csv')
    df_prices = pd.read_csv('C:\\Users\\Redmi\\PycharmProjects\\pythonProject1\\venv\\all files\\binance_prices.csv')

    # Define commission
    commission = 0.001

    # Iterate over each row in df_data
    for index, row in df_data.iterrows():
        # Get the currency to swap
        swap_currency = row['swap']

        # Get the quantity of the currency to swap
        quantity = row['amount']

        # Filter df_prices for rows where the base or quote currency is the swap currency
        df_prices_filtered = df_prices[(df_prices['base'] == swap_currency) | (df_prices['quote'] == swap_currency)]

        # Initialize variables to store the best profit and corresponding row
        best_profit = 0
        best_row = None

        # Iterate over each row in df_prices_filtered
        for index2, row2 in df_prices_filtered.iterrows():
            # Calculate the profit for this row
            if row2['base'] == swap_currency:
                # If the base currency is the swap currency, we are selling
                amount2 = quantity / row2['bidPrice'] * (1 - commission)
                usdt_equals2 = amount2 * row2['askPrice']
                profit2 = (usdt_equals2 - quantity) / quantity * 100
            else:
                # If the quote currency is the swap currency, we are buying
                amount2 = quantity * row2['askPrice'] * (1 - commission)
                usdt_equals2 = amount2 / row2['bidPrice']
                profit2 = (usdt_equals2 - quantity) / quantity * 100

            # If this profit is better than the best profit so far, update the best profit and corresponding row
            if profit2 > best_profit:
                best_profit = profit2
                best_row = row2

        # Check if a best row was found
        if best_row is not None:
            # Calculate the amount of the second currency to buy
            if best_row['base'] == swap_currency:
                amount2 = (quantity - (quantity * 0.001)) / best_row['bidPrice']
            elif best_row['quote'] == swap_currency:
                amount2 = (quantity - (quantity * 0.001)) / best_row['askPrice']
            else:
                amount2 = 0

            # Calculate the USDT equivalent of the second currency
            usdt_equals2 = amount2 * best_row['askPrice']

            # Calculate the profit of the second trade
            if best_row['base'] == swap_currency:
                profit2 = (usdt_equals2 - (usdt_equals2 * 0.001)) - quantity
            elif best_row['quote'] == swap_currency:
                profit2 = (amount2 - (amount2 * 0.001)) - (quantity / best_row['askPrice'])
            else:
                profit2 = 0

            # Update df_data with the information from the best row
            df_data.loc[index, 'pair2'] = best_row['pair']
            df_data.loc[index, 'base2'] = best_row['base']
            df_data.loc[index, 'quote2'] = best_row['quote']
            df_data.loc[index, 'bidPrice2'] = best_row['bidPrice']
            df_data.loc[index, 'bidQty2'] = best_row['bidQty']
            df_data.loc[index, 'askPrice2'] = best_row['askPrice']
            df_data.loc[index, 'askQty2'] = best_row['askQty']
            df_data.loc[index, 'swap2'] = best_row['base'] if best_row['base'] != swap_currency else best_row['quote']
            df_data.loc[index, 'amount2'] = amount2
            df_data.loc[index, 'usdt_equals2'] = usdt_equals2
            df_data.loc[index, 'profit2'] = profit2

    # Save df_data to csv
    df_data.to_csv('C:\\Users\\Redmi\\PycharmProjects\\pythonProject1\\venv\\all files\\binance_data.csv', index=False)