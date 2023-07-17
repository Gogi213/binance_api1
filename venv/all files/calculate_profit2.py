import pandas as pd
import numpy as np
from bd import connect_to_db, update_table

def calculate_profit2():
    # Load data from table
    conn = connect_to_db()
    df_data = pd.read_sql('SELECT * FROM binance_data', conn)
    df_prices = pd.read_sql('SELECT * FROM binance_prices', conn)

    # Define commission
    commission = 0.001

    # Iterate over each row in df_data
    for index, row in df_data.iterrows():
        # Get the currency to swap
        swap_currency = row['swap']

        # Get the quantity of the currency to swap
        quantity = float(row['amount'])

        # Filter df_prices for rows where the base or quote currency is the swap currency
        df_prices_filtered = df_prices[(df_prices['base'] == swap_currency) | (df_prices['quote'] == swap_currency)]

        # Iterate over each row in df_prices_filtered
        for index2, row2 in df_prices_filtered.iterrows():
            # Calculate the profit for this row
            if row2['base'] == swap_currency:  # If the base currency is the swap currency, we are selling
                amount2 = quantity / float(row2['bidprice']) * (1 - commission)
                usdt_equals2 = amount2 * float(row2['askprice'])
                profit2 = (usdt_equals2 - quantity) / quantity * 100
            else:  # If the quote currency is the swap currency, we are buying
                amount2 = quantity / float(row2['askprice']) * (1 - commission)
                usdt_equals2 = amount2 / float(row2['bidprice'])
                profit2 = (usdt_equals2 - quantity) / quantity * 100

            # Update df_data with the information from the row
            df_data.loc[index, 'pair2'] = row2['pair']
            df_data.loc[index, 'base2'] = row2['base']
            df_data.loc[index, 'quote2'] = row2['quote']
            df_data.loc[index, 'bidprice2'] = row2['bidprice']
            df_data.loc[index, 'bidqty2'] = row2['bidqty']
            df_data.loc[index, 'askprice2'] = row2['askprice']
            df_data.loc[index, 'askqty2'] = row2['askqty']
            df_data.loc[index, 'swap2'] = row2['base'] if row2['base'] != swap_currency else row2['quote']
            df_data.loc[index, 'amount2'] = amount2
            df_data.loc[index, 'usdt_equals2'] = usdt_equals2
            df_data.loc[index, 'profit2'] = profit2

        # Update table binance_data
        update_table(conn, df_data, 'binance_data')

    # Connect to the database
    conn = connect_to_db()

    # Update the 'binance_data' table
    update_table(conn, df_data, 'binance_data')