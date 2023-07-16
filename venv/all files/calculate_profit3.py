# calculate_profit3.py
import pandas as pd
from bd import connect_to_db, update_table


def calculate_profit3():
    # Load data from table
    conn = connect_to_db()
    df_data = pd.read_sql('SELECT * FROM binance_data', conn)
    df_prices = pd.read_sql('SELECT * FROM binance_prices', conn)

    # Define commission
    commission = 0.001

    # Define stablecoins
    stablecoins = ['USDT', 'USDC', 'BUSD', 'TUSD']

    # Iterate over each row in df_data
    for index, row in df_data.iterrows():
        # Get the currency to swap
        swap_currency = row['swap2']  # change to swap2 for third stage

        # Get the quantity of the currency to swap
        if row['amount2'] is not None:
            quantity = float(row['amount2'])
        else:
            print(f"Warning: amount2 is None for row {row}")
            quantity = 0.0  # or some other default value

        # Filter df_prices for rows where the base currency is the swap currency and the quote currency is a stablecoin
        df_prices_filtered = df_prices[(df_prices['base'] == swap_currency) & (df_prices['quote'].isin(stablecoins))]

        # Initialize variables to store the best profit and corresponding row
        best_profit = 0
        best_row = None

        # Iterate over each row in df_prices_filtered
        for index2, row2 in df_prices_filtered.iterrows():
            # Calculate the profit for this row
            # If the base currency is the swap currency, we are selling
            amount3 = quantity / float(row2['bidprice']) * (1 - commission)
            usdt_equals3 = amount3 * float(row2['askprice'])
            profit3 = (usdt_equals3 - quantity) / quantity * 100

            # If this profit is better than the best profit so far, update the best profit and corresponding row
            if profit3 > best_profit:
                best_profit = profit3
                best_row = row2

        # Check if a best row was found
        if best_row is not None:
            # Calculate the amount of the third currency to buy
            amount3 = (quantity - (quantity * 0.001)) / float(best_row['bidprice'])

            # Calculate the USDT equivalent of the third currency
            usdt_equals3 = amount3 * float(best_row['askprice'])

            # Calculate the profit of the third trade
            profit3 = (usdt_equals3 - (usdt_equals3 * 0.001)) - quantity

            # Update df_data with the information from the best row
            df_data.loc[index, 'pair3'] = best_row['pair']
            df_data.loc[index, 'base3'] = best_row['base']
            df_data.loc[index, 'quote3'] = best_row['quote']
            df_data.loc[index, 'bidprice3'] = best_row['bidprice']
            df_data.loc[index, 'bidqty3'] = best_row['bidqty']
            df_data.loc[index, 'askprice3'] = best_row['askprice']
            df_data.loc[index, 'askqty3'] = best_row['askqty']
            df_data.loc[index, 'swap3'] = best_row['quote']
            df_data.loc[index, 'amount3'] = amount3
            df_data.loc[index, 'usdt_equals3'] = usdt_equals3
            df_data.loc[index, 'profit3'] = profit3

    # Update table binance_data
    update_table(conn, df_data, 'binance_data')

    # Connect to the database
    conn = connect_to_db()

    # Update the 'binance_data' table
    update_table(conn, df_data, 'binance_data')
