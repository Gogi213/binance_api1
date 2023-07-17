import pandas as pd
from bd import connect_to_db, update_table

def calculate_profit3():
    # Load data from table
    conn = connect_to_db()
    df_data = pd.read_sql('SELECT * FROM binance_data', conn)
    df_prices = pd.read_sql('SELECT * FROM binance_prices', conn)

    # Define commission
    commission = 0.001

    # Define stablecoin
    stablecoin = 'USDT'

    # Iterate over each row in df_data
    for index, row in df_data.iterrows():
        # Get the currency to swap
        swap_currency = row['swap2']  # change to swap2 for third stage

        # Get the quantity of the currency to swap
        if row['amount2'] is not None:
            quantity = float(row['amount2'])
        else:
            # print(f"Warning: amount2 is None for row {row}")
            quantity = 0.0  # or some other default value

        # Filter df_prices for rows where the base currency is the swap currency and the quote currency is USDT
        df_prices_filtered = df_prices[(df_prices['base'] == swap_currency) & (df_prices['quote'] == stablecoin)]

        # Iterate over each row in df_prices_filtered
        for index2, row2 in df_prices_filtered.iterrows():
            # Calculate the profit for this row
            amount3 = quantity / float(row2['bidprice']) * (1 - commission)
            usdt_equals3 = amount3 * float(row2['askprice'])
            profit3 = (usdt_equals3 - quantity) / quantity * 100

            # Update df_data with the information from the row
            df_data.loc[index, 'pair3'] = row2['pair']
            df_data.loc[index, 'base3'] = row2['base']
            df_data.loc[index, 'quote3'] = row2['quote']
            df_data.loc[index, 'bidprice3'] = row2['bidprice']
            df_data.loc[index, 'bidqty3'] = row2['bidqty']
            df_data.loc[index, 'askprice3'] = row2['askprice']
            df_data.loc[index, 'askqty3'] = row2['askqty']
            df_data.loc[index, 'swap3'] = stablecoin
            df_data.loc[index, 'amount3'] = amount3
            df_data.loc[index, 'usdt_equals3'] = usdt_equals3
            df_data.loc[index, 'profit3'] = profit3

        # Update table binance_data
        update_table(conn, df_data, 'binance_data')

    # Connect to the database
    conn = connect_to_db()

    # Update the 'binance_data' table
    update_table(conn, df_data, 'binance_data')