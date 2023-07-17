import datetime
import concurrent.futures
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.animation as animation
from subfunctions import *
from functions import *

# Runs a simulation with the provided settings

def run_price_change_simulation(execution_index, yh_exchange, end_date, duration, budget, lot_size=1, portfolio_size=10, sensitivity=0, liquidity=0.001, reverse=False):
    # Set initial budget for future reference
    initial_budget = budget

    # Parse the end_date string to a datetime object
    end_date = datetime.datetime.strptime(end_date, "%b %d, %Y")

    # Calculate start date based on the duration (capped at Yahoo Finance Currency Historical Data oldest)
    start_date = max(end_date - datetime.timedelta(days=duration * 365), datetime.datetime.strptime("Dec 1, 2003", "%b %d, %Y"))

    # Create simulation's final dataframe
    final_df_columns = ['Date', 'Starting Amount', 'Invested Amount', 'Remainder', 'Asset Change', 'Fees', 'Profit/Loss', 'Yield', 'Bottom Line']
    final_df = pd.DataFrame(columns=final_df_columns)

    # Create simulation's log dataframe according to portfolio_size
    log_df_columns = ['Date', 'Portfolio Size', 'Starting Amount', 'Bottom Line']
    log_df_additional_columns = [f'Constituent #{i + 1}' for i in range(portfolio_size)]
    log_df_additional_columns += [f'{col} #{i + 1}' for col in ['Prediction', 'Open', 'High', 'Low', 'Close', 'Volume', 'Change', 'Quantity', 'Profit/Loss'] for i in range(portfolio_size)]
    log_df_columns += log_df_additional_columns
    log_df = pd.DataFrame(columns=log_df_columns)

    # Create execution_index dataframe, filter by start_date and remove first 50 days (50D $ Volume)
    execution_index_df = pd.read_csv(f"INDEX/{execution_index}.csv")
    execution_index_df['Date'] = pd.to_datetime(execution_index_df["Date"], format="%Y-%m-%d")
    execution_index_df = execution_index_df[execution_index_df["Date"] >= start_date].reset_index(drop=True)
    execution_index_df = execution_index_df.loc[50:].reset_index(drop=True)

    # Create list of execution symbols
    execution_symbol_list = []
    for file_name in os.listdir(yh_exchange):
        modified_name = file_name.replace(f".{yh_exchange}.csv", "")
        if modified_name not in ["Symbols", "Correct_Symbols", "Rejected_Symbols"]:
            execution_symbol_list.append(modified_name)

    # Create function to process each symbol
    def process_symbol(symbol, current_date):
        # Open symbol dataframe
        symbol_df = pd.read_csv(f"{yh_exchange}/{symbol}.{yh_exchange}.csv")
        symbol_df['Date'] = pd.to_datetime(symbol_df["Date"], format="%Y-%m-%d")

        # Get entry for current date
        symbol_entry = symbol_df[symbol_df['Date'] == current_date].reset_index(drop=True)

        # Execute following only if there is an entry for current date
        if not symbol_entry.empty:
            # Get previous entry
            symbol_entry_index = symbol_df[symbol_df['Date'] == current_date].index[0]
            previous_symbol_entry = symbol_df.iloc[symbol_entry_index - 1]

            # Execute following only if 50D $ Volume is a number
            if not np.isnan(symbol_entry['50D $ Volume'].values[0]):
                # Determine prediction
                symbol_entry_prediction = None
                if (symbol_entry['Open'].loc[0] / previous_symbol_entry['Close'] - 1) > sensitivity and not reverse:
                    symbol_entry_prediction = "H"
                elif (symbol_entry['Open'].loc[0] / previous_symbol_entry['Close'] - 1) > sensitivity and reverse:
                    symbol_entry_prediction = "L"
                elif (1 - symbol_entry['Open'].loc[0] / previous_symbol_entry['Close']) > sensitivity and not reverse:
                    symbol_entry_prediction = "L"
                elif (1 - symbol_entry['Open'].loc[0] / previous_symbol_entry['Close']) > sensitivity and reverse:
                    symbol_entry_prediction = "H"

                # Execute following only when there is a prediction
                if symbol_entry_prediction:
                    # Get maximum quantity based on liquidity
                    symbol_entry_max_qty = np.floor((symbol_entry['50D $ Volume'].values[0] * liquidity) / lot_size) * lot_size

                    # Add entry only when symbol_entry_max_qty > lot_size
                    if symbol_entry_max_qty != 0:
                        # Leave necessary columns and format them
                        symbol_entry = symbol_entry.drop(columns = ['% Day Change', '% Previous Change', '$ Volume'])
                        symbol_entry_dv = symbol_entry['50D $ Volume']
                        symbol_entry.drop('50D $ Volume', axis=1, inplace=True)
                        symbol_entry.insert(1, 'Symbol', [symbol])
                        symbol_entry.insert(2, '50D $ Volume', symbol_entry_dv)
                        symbol_entry.insert(3, 'Maximum Quantity', symbol_entry_max_qty)
                        symbol_entry.insert(4, 'Prediction', symbol_entry_prediction)

                        # Return symbol_entry to add to current_date_final_df and current_date_log_df
                        return symbol_entry

    # Create function to process each day
    def process_date(current_date):
        # Import nonlocal variables
        nonlocal final_df
        nonlocal log_df
        nonlocal budget

        # Create current_date_temp_working_df, current_date_log_df and current_date_final_df
        current_date_temp_working_df = pd.DataFrame(columns = ['Date', 'Symbol', '50D $ Volume', 'Maximum Quantity', 'Prediction', 'Open', 'High', 'Low', 'Close', 'Volume'])
        current_date_log_df = pd.DataFrame(columns=log_df_columns)
        current_date_final_df = pd.DataFrame(columns=final_df_columns)

        # Initiate multithreading
        with concurrent.futures.ThreadPoolExecutor() as executor:
            # Submit symbol processing tasks to the executor
            futures = [executor.submit(process_symbol, symbol, current_date) for symbol in execution_symbol_list]

            # Wait for all threads to finish
            concurrent.futures.wait(futures, return_when=concurrent.futures.ALL_COMPLETED)

            # Retrieve results from completed tasks
            for future in concurrent.futures.as_completed(futures):
                symbol_entry = future.result()
                current_date_temp_working_df = pd.concat([current_date_temp_working_df, symbol_entry])
        save_data(current_date_temp_working_df.sort_values(by='50D $ Volume', ascending=False), "test", "simulation")
        # Sort current_date_temp_working_df according to 50D $ Volume and portfolio_size
        current_date_temp_working_df = current_date_temp_working_df.sort_values(by='50D $ Volume', ascending=False)[:min(portfolio_size, len(current_date_temp_working_df))].reset_index(drop=True)

        # Make current_date_working_df so that we can adjust allocation if an entry doesn't meet liquidity requirements
        current_date_working_df = current_date_temp_working_df.copy()

        # Append data to current_date_log_df
        current_date_log_df.loc[0, f"Date"] = current_date
        current_date_log_df.loc[0, f"Portfolio Size"] = portfolio_size
        current_date_log_df.loc[0, f"Starting Amount"] = budget
        current_date_log_df.loc[0, f"Bottom Line"] = budget

        # Append data to current_date_final_df
        current_date_final_df.loc[0, f"Date"] = current_date
        current_date_final_df.loc[0, f"Starting Amount"] = budget
        current_date_final_df.loc[0, f"Invested Amount"] = 0
        current_date_final_df.loc[0, f"Remainder"] = 0
        current_date_final_df.loc[0, f"Asset Change"] = 0
        current_date_final_df.loc[0, f"Fees"] = 0
        current_date_final_df.loc[0, f"Profit/Loss"] = 0
        current_date_final_df.loc[0, f"Yield"] = 0
        current_date_final_df.loc[0, f"Bottom Line"] = budget

        # Loop through current_date_temp_working_df and add entries to current_date_log_df
        for index, entry in current_date_temp_working_df.iterrows():
            # Assign prediction for entry
            entry_prediction = entry['Prediction']

            # Assign 50D $ Volume and Maximum Quantity for entry
            entry_dv = entry['50D $ Volume']
            entry_max_qty = entry['Maximum Quantity']

            # Calculate allocation for the entry
            entry_portfolio_percentage = entry_dv / current_date_temp_working_df['50D $ Volume'].sum()
            entry_allocation = entry_portfolio_percentage * budget

            # Calculate quantity for the entry
            entry_quantity = np.floor(entry_allocation / (entry['Open'] * lot_size)) * lot_size
            print(entry_max_qty)
            # Only add entry if it meets the liquidity requirement
            # Or else, delete it from future allocation calculations and add additional entry
            if entry_quantity <= entry_max_qty:
                # Append data to current_date_log_df
                current_date_log_df.loc[0, f"Constituent #{index + 1}"] = entry['Symbol']
                current_date_log_df.loc[0, f"Prediction #{index + 1}"] = entry_prediction
                current_date_log_df.loc[0, f"Open #{index + 1}"] = entry['Open']
                current_date_log_df.loc[0, f"High #{index + 1}"] = entry['High']
                current_date_log_df.loc[0, f"Low #{index + 1}"] = entry['Low']
                current_date_log_df.loc[0, f"Close #{index + 1}"] = entry['Close']
                current_date_log_df.loc[0, f"Volume #{index + 1}"] = entry['Volume']
                current_date_log_df.loc[0, f"Change #{index + 1}"] = round((entry['Close'] - entry['Open']) / entry['Open'], 4)
                current_date_log_df.loc[0, f"Quantity #{index + 1}"] = entry_quantity
                # Assign asset change to Profit/Loss
                if entry_prediction == "H":
                    current_date_log_df.loc[0, f"Profit/Loss #{index + 1}"] = (entry['Close'] - entry['Open']) * current_date_log_df.loc[0, f"Quantity #{index + 1}"]
                elif entry_prediction == "L":
                    current_date_log_df.loc[0, f"Profit/Loss #{index + 1}"] = (entry['Open'] - entry['Close']) * current_date_log_df.loc[0, f"Quantity #{index + 1}"]
                # Deduct fees from Profit/Loss
                current_date_log_df.loc[0, f"Profit/Loss #{index + 1}"] -= ibkr_sgx_fees(entry['Open'] * current_date_log_df.loc[0, f"Quantity #{index + 1}"], "fixed")
                current_date_log_df.loc[0, f"Profit/Loss #{index + 1}"] -= ibkr_sgx_fees(entry['Close'] * current_date_log_df.loc[0, f"Quantity #{index + 1}"],"fixed")
                # Calculate and append Bottom Line
                current_date_log_df.loc[0, f"Bottom Line"] += current_date_log_df.loc[0, f"Profit/Loss #{index + 1}"]
    
                # Append data to current_date_final_df
                current_date_final_df.loc[0, f"Invested Amount"] += current_date_log_df.loc[0, f"Open #{index + 1}"] * current_date_log_df.loc[0, f"Quantity #{index + 1}"]
                current_date_final_df.loc[0, f"Remainder"] += entry_allocation - current_date_log_df.loc[0, f"Open #{index + 1}"] * current_date_log_df.loc[0, f"Quantity #{index + 1}"]
                # Assign asset change
                if entry_prediction == "H":
                    current_date_final_df.loc[0, f"Asset Change"] += (entry['Close'] - entry['Open']) * current_date_log_df.loc[0, f"Quantity #{index + 1}"]
                elif entry_prediction == "L":
                    current_date_final_df.loc[0, f"Asset Change"] += (entry['Open'] - entry['Close']) * current_date_log_df.loc[0, f"Quantity #{index + 1}"]
                current_date_final_df.loc[0, f"Fees"] += ibkr_sgx_fees(entry['Open'] * current_date_log_df.loc[0, f"Quantity #{index + 1}"], "fixed") + ibkr_sgx_fees(entry['Close'] * current_date_log_df.loc[0, f"Quantity #{index + 1}"],"fixed")
                current_date_final_df.loc[0, f"Profit/Loss"] += current_date_log_df.loc[0, f"Profit/Loss #{index + 1}"]
                current_date_final_df.loc[0, f"Yield"] += entry_portfolio_percentage * current_date_log_df.loc[0, f"Profit/Loss #{index + 1}"] / entry_allocation
                current_date_final_df.loc[0, f"Bottom Line"] += current_date_log_df.loc[0, f"Profit/Loss #{index + 1}"]

        # Add the 2 current_date dataframes to the full dataframes
        final_df = pd.concat([final_df, current_date_final_df])
        log_df = pd.concat([log_df, current_date_log_df])

        # Set budget to bottom line
        budget = max(current_date_final_df.loc[0, f"Bottom Line"], 0)

        # Print status update
        print(f"[{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {current_date.strftime('%Y-%m-%d')} Daily Yield: {round(current_date_final_df.loc[0, 'Yield'] * 100, 2)}% | Balance: ${round(current_date_final_df.loc[0, 'Bottom Line'], 2)}")

    for date in execution_index_df['Date']:
        # Run process_date for current date
        process_date(date)

        # # Graph cumulative final_df performance
        # final_df_x = [final_df.iloc[0, 0]]
        # final_df_y = [initial_budget]
        # final_df_x.extend(final_df['Date'])
        # final_df_y.extend(final_df['Bottom Line'])

        # Save cumulative dataframes
        save_data(log_df, f"price_change_{execution_index}_exec_{end_date.strftime('%Y-%m-%d')}_date_{duration}_dura_{initial_budget}_budg_{portfolio_size}_size_{sensitivity}_sens_{reverse}_reve_log_df", "simulations")
        save_data(final_df, f"price_change_{execution_index}_exec_{end_date.strftime('%Y-%m-%d')}_date_{duration}_dura_{initial_budget}_budg_{portfolio_size}_size_{sensitivity}_sens_{reverse}_reve_final_df", "simulations")
        break
def main():

    execution_index = "%5ESTI"
    yh_exchange = "SI"
    end_date = "Jul 12, 2023"
    duration = 20
    budget = 100000
    lot_size = 100
    portfolio_size = 10
    sensitivity = 0.02
    liquidity = 0.005
    reverse = True

    time_function(run_price_change_simulation, execution_index, yh_exchange, end_date, duration, budget, lot_size, portfolio_size, sensitivity, liquidity, reverse)

if __name__ == "__main__":
    main()