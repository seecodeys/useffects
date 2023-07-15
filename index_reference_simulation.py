import datetime
import concurrent.futures
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.animation as animation
from subfunctions import *
from functions import *

# Runs a simulation with the provided settings

def run_index_reference_simulation(execution_index, reference_index, yh_exchange, end_date, duration, budget, lot_size=1, portfolio_size=10, reverse=False):
    # Set initial budget for future reference
    initial_budget = budget

    # Parse the end_date string to a datetime object
    end_date = datetime.datetime.strptime(end_date, "%b %d, %Y")

    # Calculate start date based on the duration (capped at Yahoo Finance Currency Historical Data oldest)
    start_date = max(end_date - datetime.timedelta(days=duration * 365), datetime.datetime.strptime("Dec 1, 2003", "%b %d, %Y"))

    # Create simulation's final dataframe
    final_df_columns = ['Date', 'Prediction', 'Starting Amount', 'Invested Amount', 'Remainder', 'Asset Change', 'Fees', 'Profit/Loss', 'Yield', 'Bottom Line']
    final_df = pd.DataFrame(columns=final_df_columns)

    # Create simulation's log dataframe according to portfolio_size
    log_df_columns = ['Date', 'Prediction', 'Portfolio Size', 'Starting Amount', 'Bottom Line']
    log_df_additional_columns = [f'Constituent #{i + 1}' for i in range(portfolio_size)]
    log_df_additional_columns += [f'{col} #{i + 1}' for col in ['Open', 'High', 'Low', 'Close', 'Volume', 'Change', 'Quantity', 'Profit/Loss'] for i in range(portfolio_size)]
    log_df_columns += log_df_additional_columns
    log_df = pd.DataFrame(columns=log_df_columns)

    # Create execution_index dataframe, filter by start_date and remove first 50 days (50D $ Volume)
    execution_index_df = pd.read_csv(f"INDEX/{execution_index}.csv")
    execution_index_df['Date'] = pd.to_datetime(execution_index_df["Date"], format="%Y-%m-%d")
    execution_index_df = execution_index_df[execution_index_df["Date"] >= start_date].reset_index(drop=True)
    execution_index_df = execution_index_df.loc[50:].reset_index(drop=True)

    # Create reference_index dataframe
    reference_index_df = pd.read_csv(f"INDEX/{reference_index}.csv")
    reference_index_df['Date'] = pd.to_datetime(reference_index_df["Date"], format="%Y-%m-%d")

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

        # Execute following if there is an entry for current date
        if not symbol_entry.empty:
            # Leave necessary columns and format them
            symbol_entry = symbol_entry.drop(columns = ['% Day Change', '% Previous Change', '$ Volume'])
            symbol_entry_dv = symbol_entry['50D $ Volume']
            symbol_entry.drop('50D $ Volume', axis=1, inplace=True)
            symbol_entry.insert(1, 'Symbol', [symbol])
            symbol_entry.insert(2, '50D $ Volume', symbol_entry_dv)

            # Return symbol_entry to add to current_date_final_df and current_date_log_df
            return symbol_entry

    # Create function to process each day
    def process_date(current_date):
        # Import nonlocal variables
        nonlocal final_df
        nonlocal log_df
        nonlocal budget

        # Get the closest reference date that is <= current_date
        current_date_reference_index = None
        smaller_dates = reference_index_df.loc[reference_index_df['Date'] < current_date, 'Date']
        if len(smaller_dates) > 0:
            nearest_date = np.max(smaller_dates)
            current_date_reference_index = reference_index_df.index[reference_index_df['Date'] == nearest_date][0]

        # Assign change to variable
        reference_index_change = reference_index_df.iloc[current_date_reference_index, 6]

        # Apply reverse
        if reverse:
            reference_index_change= reference_index_change * -1

        # Assign "H" or "L" to variable based on numerical change
        if reference_index_change >= 0:
            reference_index_change = "H"
        elif reference_index_change < 0:
            reference_index_change = "L"

        # Create current_date_working_df, current_date_log_df and current_date_final_df
        current_date_working_df = pd.DataFrame(columns = ['Date', 'Symbol', '50D $ Volume', 'Open', 'High', 'Low', 'Close', 'Volume'])
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
                current_date_working_df = pd.concat([current_date_working_df, symbol_entry])

        # Sort current_date_working_df according to 50D $ Volume and portfolio_size
        current_date_working_df = current_date_working_df.sort_values(by='50D $ Volume', ascending=False)[:min(portfolio_size, len(current_date_working_df))].reset_index(drop=True)

        # Append data to current_date_log_df
        current_date_log_df.loc[0, f"Date"] = current_date
        current_date_log_df.loc[0, f"Prediction"] = reference_index_change
        current_date_log_df.loc[0, f"Portfolio Size"] = portfolio_size
        current_date_log_df.loc[0, f"Starting Amount"] = budget
        current_date_log_df.loc[0, f"Bottom Line"] = budget

        # Append data to current_date_final_df
        current_date_final_df.loc[0, f"Date"] = current_date
        current_date_final_df.loc[0, f"Prediction"] = reference_index_change
        current_date_final_df.loc[0, f"Starting Amount"] = budget
        current_date_final_df.loc[0, f"Invested Amount"] = 0
        current_date_final_df.loc[0, f"Remainder"] = 0
        current_date_final_df.loc[0, f"Asset Change"] = 0
        current_date_final_df.loc[0, f"Fees"] = 0
        current_date_final_df.loc[0, f"Profit/Loss"] = 0
        current_date_final_df.loc[0, f"Yield"] = 0
        current_date_final_df.loc[0, f"Bottom Line"] = budget

        # Loop through current_date_working_df and add entries to current_date_log_df
        for index, entry in current_date_working_df.iterrows():
            # Calculate allocation for the entry
            entry_portfolio_percentage = entry['50D $ Volume'] / current_date_working_df['50D $ Volume'].sum()
            entry_allocation = entry_portfolio_percentage * budget

            # Append data to current_date_log_df
            current_date_log_df.loc[0, f"Constituent #{index + 1}"] = entry['Symbol']
            current_date_log_df.loc[0, f"Open #{index + 1}"] = entry['Open']
            current_date_log_df.loc[0, f"High #{index + 1}"] = entry['High']
            current_date_log_df.loc[0, f"Low #{index + 1}"] = entry['Low']
            current_date_log_df.loc[0, f"Close #{index + 1}"] = entry['Close']
            current_date_log_df.loc[0, f"Volume #{index + 1}"] = entry['Volume']
            current_date_log_df.loc[0, f"Change #{index + 1}"] = round((entry['Close'] - entry['Open']) / entry['Open'], 4)
            current_date_log_df.loc[0, f"Quantity #{index + 1}"] = np.floor(entry_allocation / (entry['Open'] * lot_size)) * lot_size
            # Assign asset change to Profit/Loss
            if reference_index_change == "H":
                current_date_log_df.loc[0, f"Profit/Loss #{index + 1}"] = (entry['Close'] - entry['Open']) * current_date_log_df.loc[0, f"Quantity #{index + 1}"]
            elif reference_index_change == "L":
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
            if reference_index_change == "H":
                current_date_final_df.loc[0, f"Asset Change"] += (entry['Close'] - entry['Open']) * current_date_log_df.loc[0, f"Quantity #{index + 1}"]
            elif reference_index_change == "L":
                current_date_final_df.loc[0, f"Asset Change"] += (entry['Open'] - entry['Close']) * current_date_log_df.loc[0, f"Quantity #{index + 1}"]
            current_date_final_df.loc[0, f"Fees"] += ibkr_sgx_fees(entry['Open'] * current_date_log_df.loc[0, f"Quantity #{index + 1}"], "fixed") + ibkr_sgx_fees(entry['Close'] * current_date_log_df.loc[0, f"Quantity #{index + 1}"],"fixed")
            current_date_final_df.loc[0, f"Profit/Loss"] += current_date_log_df.loc[0, f"Profit/Loss #{index + 1}"]
            current_date_final_df.loc[0, f"Yield"] += entry_portfolio_percentage * current_date_log_df.loc[0, f"Profit/Loss #{index + 1}"] / entry_allocation
            current_date_final_df.loc[0, f"Bottom Line"] += current_date_log_df.loc[0, f"Profit/Loss #{index + 1}"]

        # Add the 2 current_date dataframes to the full dataframes
        final_df = pd.concat([final_df, current_date_final_df])
        log_df = pd.concat([log_df, current_date_log_df])

        # Set budget to bottom line
        budget = current_date_final_df.loc[0, f"Bottom Line"]

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
        save_data(log_df, f"{execution_index}_exec_{reference_index}_refe_{end_date.strftime('%Y-%m-%d')}_date_{duration}_dura_{initial_budget}_budg_{portfolio_size}_size_{reverse}_reve_log_df", "simulations")
        save_data(final_df, f"{execution_index}_exec_{reference_index}_refe_{end_date.strftime('%Y-%m-%d')}_date_{duration}_dura_{initial_budget}_budg_{portfolio_size}_size_{reverse}_reve_final_df", "simulations")

def main():

    execution_index = "%5ESTI"
    reference_index = "%5EGSPC"
    yh_exchange = "SI"
    end_date = "Jul 12, 2023"
    duration = 20
    budget = 25000
    lot_size = 100
    portfolio_size = 1
    reverse = True

    time_function(run_index_reference_simulation, execution_index, reference_index, yh_exchange, end_date, duration, budget, lot_size, portfolio_size, reverse)

if __name__ == "__main__":
    main()