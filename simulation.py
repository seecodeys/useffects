import concurrent.futures
import csv
import datetime
import threading
import os

import numpy as np
import pandas as pd

from functions import *
from subfunctions import *

# NEEDS MAJOR REWORK

def daily_order(index, exchange, end_date, duration, budget, lot_size=1, portfolio_size=float('inf')):
    starting_budget = budget

    # Parse the end_date string to a datetime object
    end_date = datetime.datetime.strptime(end_date, "%b %d, %Y")

    # Calculate start date based on the duration (capped at Yahoo Finance Currency Historical Data oldest)
    start_date = max(end_date - datetime.timedelta(days=duration * 365), datetime.datetime.strptime("Dec 1, 2003", "%b %d, %Y"))

    # Create simulation dataframe
    df_columns = ['Date', 'Allocation', 'Investment', 'Remainder', 'Fees', 'Asset Change', 'Profit/Loss', 'Balance']
    df = pd.DataFrame(columns=df_columns)

    index_file_path = os.path.join(os.getcwd(), f"INDEX/{index}.csv")
    index_df = pd.read_csv(index_file_path)
    index_df['Date'] = pd.to_datetime(index_df["Date"], format="%d/%m/%Y")
    index_df = index_df[index_df["Date"] >= start_date].reset_index(drop=True)
    index_df = index_df.loc[50:].reset_index(drop=True)
    symbol_file_path = os.path.join(os.getcwd(), f'{exchange}/Correct_Symbols.{exchange}.csv')
    symbol_column_index = 0  # Index of the column to extract
    us_file_path = os.path.join(os.getcwd(), f"INDEX/spx.csv")
    us_df = pd.read_csv(us_file_path)
    us_df['Date'] = pd.to_datetime(us_df["Date"], format="%d/%m/%Y")

    # Create a lock object
    lock = threading.Lock()
    current_date = ""
    current_df = pd.DataFrame(columns=['Symbol', '50D $ Volume'])
    # Read the CSV file
    with open(symbol_file_path, 'r') as file:
        csv_reader = csv.reader(file)
        # Extract values from the first column into a list
        symbols_list = list(set([row[symbol_column_index] for row in csv_reader]))

        def process_symbol(symbol):
            nonlocal current_df
            stock_df = pd.read_csv(os.path.join(os.getcwd(), f"SI/{symbol}.SI.csv"))
            stock_entry_index = stock_df.index[stock_df['Date'] == current_date][0]
            stock_dv = 0
            stock_change = 0
            price = 0
            try:
                stock_dv = stock_df.iloc[stock_entry_index, 9]
                stock_change = stock_df.iloc[stock_entry_index, 6]
                price = stock_df.iloc[stock_entry_index, 1] # Assuming purchase price is closer to open than previous close
            except:
                pass
            if stock_dv > 0:
                with lock:
                    current_df = current_df.append({
                        'Symbol': symbol,
                        '50D $ Volume': stock_dv,
                        '% Day Change': stock_change,
                        'Price': price
                    }, ignore_index=True)

        for index in range(len(index_df)):
            current_date = index_df.loc[index, 'Date'].strftime("%Y-%m-%d")

            with concurrent.futures.ThreadPoolExecutor() as executor:
                # Submit symbol processing tasks to the executor
                futures = [executor.submit(process_symbol, symbol) for symbol in symbols_list]

                # Wait for all tasks to complete
                concurrent.futures.wait(futures)

            current_df = current_df.sort_values(by='50D $ Volume', ascending=False)[:min(portfolio_size, len(current_df))].reset_index(drop=True)

            #####################################################

            current_df['Allocation'] = current_df['50D $ Volume'] / current_df['50D $ Volume'].sum() * budget
            current_df['Quantity'] = np.floor(current_df['Allocation'] / (current_df['Price'] * lot_size)) * lot_size
            current_df['Investment'] = round(current_df['Quantity'] * current_df['Price'], 2)
            current_df['Remainder'] = current_df['Allocation'] - current_df['Investment']
            current_df['Fees'] = current_df['Investment'].apply(lambda x: ibkr_sgx_fees(x, "fixed"))

            us_entry_index = 0

            try:
                us_entry_index = us_df.index[us_df['Date'] == current_date][0]
            except:
                pass

            if us_entry_index == 0:
                smaller_dates = us_df.loc[us_df['Date'] < current_date, 'Date']
                if len(smaller_dates) > 0:
                    nearest_date = np.max(smaller_dates)
                    us_entry_index = us_df.index[us_df['Date'] == nearest_date][0]

            us_change = us_df.iloc[us_entry_index - 1, 5]

            if us_change >= 0:
                current_df['Asset Change'] = round((current_df['Investment'] * current_df['% Day Change']), 2)
            else:
                current_df['Asset Change'] = round((current_df['Investment'] * current_df['% Day Change']), 2) * -1

            current_df['Fees'] = current_df['Fees'] + current_df['Investment'].apply(lambda x: ibkr_sgx_fees(x, "fixed"))
            current_df['Profit/Loss'] = current_df['Asset Change'] - current_df['Fees']
            current_df['Balance'] = current_df['Investment'] + current_df['Asset Change'] - current_df['Fees'] + current_df['Remainder']
            save_data(current_df, current_date, "simulations", True)
            final_df_columns = ['Allocation', 'Investment', 'Remainder', 'Fees', 'Asset Change', 'Profit/Loss','Balance']
            final_current_df = current_df.sum(axis=0)[final_df_columns]
            final_current_df['Date'] = current_date
            df = df.append(final_current_df, ignore_index=True)

            print(f"[{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {final_current_df['Date']} Daily Yield: {round((final_current_df['Profit/Loss'] / final_current_df['Allocation']) * 100, 2)}% | Balance: ${round(final_current_df['Balance'], 2)}")
            save_data(df, f"{duration}year_{starting_budget}money_{portfolio_size}holdings", "simulations", True)

            current_df = current_df.drop(current_df.index)
            budget = final_current_df['Balance']

def main():

    index = "sti"
    exchange = "SI"
    end_date = "Jul 07, 2023"
    duration = 15
    budget = 25000
    lot_size = 100
    portfolio_size = 1

    time_function(daily_order, index, exchange, end_date, duration, budget, lot_size, portfolio_size)

if __name__ == "__main__":
    main()