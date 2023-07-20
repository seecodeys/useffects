import datetime
import concurrent.futures
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.animation as animation
from subfunctions import *
from functions import *


# Runs a simulation with the provided settings

def run_price_change_simulation(execution_index, folder, end_date, duration, budget, lot_size=1, sensitivity=0, liquidity=0.001, stop_loss=float('inf'), max_fee=0.002, ibkr_pricing_mode="tiered", monthly_trade_volume=0, reverse=False):
    # Set initial budget for future reference
    initial_budget = budget

    # Parse the end_date string to a datetime object
    end_date = datetime.datetime.strptime(end_date, "%b %d, %Y")

    # Calculate start date based on the duration (capped at Yahoo Finance Currency Historical Data oldest)
    start_date = max(end_date - datetime.timedelta(days=duration * 365),
                     datetime.datetime.strptime("Dec 1, 2003", "%b %d, %Y"))

    # Create list of execution symbols
    execution_symbol_list = []
    for file_name in os.listdir(folder):
        modified_name = file_name.split(".")[0]
        if modified_name not in ["Symbols", "Correct_Symbols", "Rejected_Symbols"]:
            execution_symbol_list.append(modified_name)

    # Create simulation's final dataframe
    final_df_columns = ['Date', 'Starting Amount', 'Invested Amount', 'Remainder', 'Asset Change', 'Fees',
                        'Profit/Loss', 'Yield', 'Bottom Line']
    final_df = pd.DataFrame(columns=final_df_columns)

    # Create simulation's log dataframe according to number of stocks
    log_df_columns = ['Date', 'Portfolio Size', 'Starting Amount', 'Bottom Line']
    log_df_additional_columns = [f'Constituent #{i + 1}' for i in range(len(execution_symbol_list))]
    log_df_additional_columns += [f'{col} #{i + 1}' for col in ['Prediction', 'Open', 'High', 'Low', 'Close', 'Volume', 'Change', 'Quantity', 'Stop Loss Triggered', 'Profit/Loss'] for i in range(len(execution_symbol_list))]
    log_df_columns += log_df_additional_columns
    log_df = pd.DataFrame(columns=log_df_columns)

    # Create execution_index dataframe, filter by start_date and remove first 10 days (Previous 10D $ Volume)
    execution_index_df = pd.read_csv(f"INDEX/{execution_index}.csv")
    execution_index_df['Date'] = pd.to_datetime(execution_index_df["Date"], format="%Y-%m-%d")
    execution_index_df = execution_index_df[execution_index_df["Date"] >= start_date].reset_index(drop=True)
    execution_index_df = execution_index_df.loc[10:].reset_index(drop=True)

    # Create function to process each symbol
    def process_symbol(symbol, current_date):
        # Open symbol dataframe
        symbol_df = pd.read_csv(f"{folder}/{symbol}.csv")
        symbol_df['Date'] = pd.to_datetime(symbol_df["Date"], format="%Y-%m-%d")

        # Get entry for current date
        symbol_entry = symbol_df[symbol_df['Date'] == current_date].reset_index(drop=True)

        # Execute following only if there is an entry for current date
        if not symbol_entry.empty:
            # Get the previous 10D entries
            symbol_entry_index = symbol_df[symbol_df['Date'] == current_date].index[0]
            previous_symbol_entries = symbol_df.iloc[symbol_entry_index - 10:symbol_entry_index]

            # Execute following only if no 0 is found in the price columns
            if not (previous_symbol_entries[['Open', 'High', 'Low', 'Close']] == 0).any().any():
                # Get previous entry
                previous_symbol_entry = symbol_df.iloc[symbol_entry_index - 1]

                # Execute following only if Previous 10D $ Volume is a number
                if not np.isnan(symbol_entry['Previous 10D $ Volume'].values[0]):
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
                        symbol_entry_max_qty = np.floor((previous_symbol_entries['$ Volume'].min() * liquidity) / lot_size) * lot_size
                        # Add entry only when symbol_entry_max_qty > lot_size
                        if symbol_entry_max_qty != 0:
                            # Leave necessary columns and format them
                            symbol_entry = symbol_entry.drop(columns=['% Day Change', '% Previous Change', '$ Volume'])
                            symbol_entry_dv = symbol_entry['Previous 10D $ Volume']
                            symbol_entry.drop('Previous 10D $ Volume', axis=1, inplace=True)
                            symbol_entry.insert(1, 'Symbol', [symbol])
                            symbol_entry.insert(2, 'Previous 10D $ Volume', symbol_entry_dv)
                            symbol_entry.insert(3, 'Prediction', symbol_entry_prediction)
    
                            # Return symbol_entry to add to current_date_final_df and current_date_log_df
                            return symbol_entry

    # Create function to process each day
    def process_date(current_date):
        # Import nonlocal variables
        nonlocal final_df
        nonlocal log_df
        nonlocal budget

        # Create current_date_temp_working_df, current_date_log_df and current_date_final_df
        current_date_temp_working_df = pd.DataFrame(columns=['Date', 'Symbol', 'Previous 10D $ Volume', 'Prediction', 'Open', 'High', 'Low', 'Close', 'Volume'])
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

        # Sort current_date_temp_working_df according to Previous 10D $ Volume
        current_date_temp_working_df = current_date_temp_working_df.sort_values(by='Previous 10D $ Volume', ascending=False).reset_index(drop=True)

        # Reoptimize portfolio to eliminate those where quantity = 0
        current_date_temp_working_df['Allocation'] = current_date_temp_working_df['Previous 10D $ Volume'] / current_date_temp_working_df['Previous 10D $ Volume'].sum() * budget
        current_date_temp_working_df['Quantity'] = np.floor(current_date_temp_working_df['Allocation'] / current_date_temp_working_df['Open'])
        if (current_date_temp_working_df['Quantity'] == 0).any():
            current_date_temp_working_df = current_date_temp_working_df.loc[:current_date_temp_working_df[current_date_temp_working_df['Quantity'] == 0].index[0]]
            current_date_temp_working_df['Allocation'] = current_date_temp_working_df['Previous 10D $ Volume'] / current_date_temp_working_df['Previous 10D $ Volume'].sum() * budget
            current_date_temp_working_df['Quantity'] = np.floor(current_date_temp_working_df['Allocation'] / current_date_temp_working_df['Open'])
            if current_date_temp_working_df['Quantity'].iloc[-1] == 0:
                current_date_temp_working_df = current_date_temp_working_df.iloc[:-1]
                current_date_temp_working_df['Allocation'] = current_date_temp_working_df['Previous 10D $ Volume'] / current_date_temp_working_df['Previous 10D $ Volume'].sum() * budget
                current_date_temp_working_df['Quantity'] = np.floor(current_date_temp_working_df['Allocation'] / current_date_temp_working_df['Open'])

        # Reoptimize portfolio to eliminate those where fees > max_fees
        while True:
            current_date_temp_working_df['Investment'] = current_date_temp_working_df['Quantity'] * current_date_temp_working_df['Open']
            current_date_temp_working_df['Fees'] = current_date_temp_working_df.apply(lambda entry: 2 * ibkr_us_fees(entry['Open'], entry['Quantity'], ibkr_pricing_mode, monthly_trade_volume), axis=1)
            current_date_temp_working_df['Fee Percentage'] = current_date_temp_working_df['Fees'] / current_date_temp_working_df['Investment']
            if (current_date_temp_working_df['Fee Percentage'] > max_fee).any():
                current_date_temp_working_df = current_date_temp_working_df.iloc[:-1]
                current_date_temp_working_df['Allocation'] = current_date_temp_working_df['Previous 10D $ Volume'] / current_date_temp_working_df['Previous 10D $ Volume'].sum() * budget
                current_date_temp_working_df['Quantity'] = np.floor(current_date_temp_working_df['Allocation'] / current_date_temp_working_df['Open'])
            else:
                break

        # Append data to current_date_log_df
        current_date_log_df.loc[0, f"Date"] = current_date
        current_date_log_df.loc[0, f"Portfolio Size"] = len(current_date_temp_working_df)
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

        # Initiate stop loss counter for observation
        stop_loss_count = 0

        # Loop through current_date_temp_working_df and add entries to current_date_log_df
        for index, entry in current_date_temp_working_df.iterrows():
            # Assign prediction for entry
            entry_prediction = entry['Prediction']

            # Append data to current_date_log_df
            current_date_log_df.loc[0, f"Constituent #{index + 1}"] = entry['Symbol']
            current_date_log_df.loc[0, f"Prediction #{index + 1}"] = entry_prediction
            current_date_log_df.loc[0, f"Open #{index + 1}"] = entry['Open']
            current_date_log_df.loc[0, f"High #{index + 1}"] = entry['High']
            current_date_log_df.loc[0, f"Low #{index + 1}"] = entry['Low']
            current_date_log_df.loc[0, f"Close #{index + 1}"] = entry['Close']
            current_date_log_df.loc[0, f"Volume #{index + 1}"] = entry['Volume']
            current_date_log_df.loc[0, f"Change #{index + 1}"] = round((entry['Close'] - entry['Open']) / entry['Open'], 4)
            current_date_log_df.loc[0, f"Quantity #{index + 1}"] = entry['Quantity']
            # Assign asset change to Profit/Loss
            if entry_prediction == "H":
                # Implement stop losses
                if 1 - entry['Low'] / entry['Open'] > stop_loss:
                    current_date_log_df.loc[0, f"Profit/Loss #{index + 1}"] = (entry['Open'] * stop_loss) * entry['Quantity'] * -1
                    current_date_log_df.loc[0, f"Stop Loss Triggered #{index + 1}"] = True
                    stop_loss_count += 1
                else:
                    current_date_log_df.loc[0, f"Profit/Loss #{index + 1}"] = (entry['Close'] - entry['Open']) * entry['Quantity']
                    current_date_log_df.loc[0, f"Stop Loss Triggered #{index + 1}"] = False
            elif entry_prediction == "L":
                # Implement stop losses
                if entry['High'] / entry['Open'] - 1 > stop_loss:
                    current_date_log_df.loc[0, f"Profit/Loss #{index + 1}"] = (entry['Open'] * stop_loss) * entry['Quantity'] * -1
                    current_date_log_df.loc[0, f"Stop Loss Triggered #{index + 1}"] = True
                    stop_loss_count += 1
                else:
                    current_date_log_df.loc[0, f"Profit/Loss #{index + 1}"] = (entry['Open'] - entry['Close']) * entry['Quantity']
                    current_date_log_df.loc[0, f"Stop Loss Triggered #{index + 1}"] = False
            # Deduct fees from Profit/Loss
            current_date_log_df.loc[0, f"Profit/Loss #{index + 1}"] -= ibkr_us_fees(entry['Open'], entry['Quantity'], ibkr_pricing_mode, monthly_trade_volume)
            # Implement stop losses
            if entry_prediction == "H" and 1 - entry['Low'] / entry['Open'] >= stop_loss:
                current_date_log_df.loc[0, f"Profit/Loss #{index + 1}"] -= ibkr_us_fees(entry['Open'] * (1 - stop_loss), entry['Quantity'], ibkr_pricing_mode, monthly_trade_volume)
            # Implement stop losses
            elif entry_prediction == "L" and entry['High'] / entry['Open'] >= stop_loss:
                current_date_log_df.loc[0, f"Profit/Loss #{index + 1}"] -= ibkr_us_fees(entry['Open'] * (1 + stop_loss), entry['Quantity'], ibkr_pricing_mode, monthly_trade_volume)
            else:
                current_date_log_df.loc[0, f"Profit/Loss #{index + 1}"] -= ibkr_us_fees(entry['Close'], entry['Quantity'], ibkr_pricing_mode, monthly_trade_volume)
            # Calculate and append Bottom Line
            current_date_log_df.loc[0, f"Bottom Line"] += current_date_log_df.loc[0, f"Profit/Loss #{index + 1}"]

            # Append data to current_date_final_df
            current_date_final_df.loc[0, f"Invested Amount"] += entry['Investment']
            current_date_final_df.loc[0, f"Remainder"] += entry['Allocation'] - entry['Investment']
            # Assign asset change
            if entry_prediction == "H":
                current_date_final_df.loc[0, f"Asset Change"] += (entry['Close'] - entry['Open']) * current_date_log_df.loc[0, f"Quantity #{index + 1}"]
            elif entry_prediction == "L":
                current_date_final_df.loc[0, f"Asset Change"] += (entry['Open'] - entry['Close']) * current_date_log_df.loc[0, f"Quantity #{index + 1}"]
            current_date_final_df.loc[0, f"Fees"] += ibkr_us_fees(entry['Open'], entry['Quantity'], ibkr_pricing_mode, monthly_trade_volume) + ibkr_us_fees(entry['Close'], entry['Quantity'], ibkr_pricing_mode, monthly_trade_volume)
            current_date_final_df.loc[0, f"Profit/Loss"] += current_date_log_df.loc[0, f"Profit/Loss #{index + 1}"]
            current_date_final_df.loc[0, f"Yield"] += current_date_log_df.loc[0, f"Profit/Loss #{index + 1}"] / budget
            current_date_final_df.loc[0, f"Bottom Line"] += current_date_log_df.loc[0, f"Profit/Loss #{index + 1}"]

        # Add the 2 current_date dataframes to the full dataframes
        final_df = pd.concat([final_df, current_date_final_df])
        log_df = pd.concat([log_df, current_date_log_df])

        # Set budget to bottom line
        budget = max(current_date_final_df.loc[0, f"Bottom Line"], 0)

        # Print status update
        print(f"[{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {current_date.strftime('%Y-%m-%d')} Daily Yield: {round(current_date_final_df.loc[0, 'Yield'] * 100, 2)}% | Balance: ${round(current_date_final_df.loc[0, 'Bottom Line'], 2)} | {stop_loss_count}/{len(current_date_temp_working_df)} Stopped Out")

    for date in execution_index_df['Date']:
        # Run process_date for current date
        process_date(date)

        # if date == pd.to_datetime("2003/12/17", format="%Y/%m/%d"):
        #     print(date)
        #     # Run process_date for current date
        #     process_date(date)
    #
    #     # # Graph cumulative final_df performance
    #     # final_df_x = [final_df.iloc[0, 0]]
    #     # final_df_y = [initial_budget]
    #     # final_df_x.extend(final_df['Date'])
    #     # final_df_y.extend(final_df['Bottom Line'])
    #
        # Save cumulative dataframes
        save_data(log_df, f"price_change_{execution_index}_exec_{end_date.strftime('%Y-%m-%d')}_date_{duration}_dura_{initial_budget}_budg_{sensitivity}_sens_{liquidity}_liqu_{stop_loss}_stop_{max_fee}_maxf_{ibkr_pricing_mode}_pmod_{monthly_trade_volume}_motv_{reverse}_reve_log_df", "simulations")
        save_data(final_df, f"price_change_{execution_index}_exec_{end_date.strftime('%Y-%m-%d')}_date_{duration}_dura_{initial_budget}_budg_{sensitivity}_sens_{liquidity}_liqu_{stop_loss}_stop_{max_fee}_maxf_{ibkr_pricing_mode}_pmod_{monthly_trade_volume}_motv_{reverse}_reve_final_df", "simulations")


def main():
    execution_index = "%5EGSPC"
    folder = "US"
    end_date = "Jul 15, 2023"
    duration = 20
    budget = 100000
    lot_size = 1
    sensitivity = 0.02
    liquidity = 0.000001
    stop_loss = 0.02
    max_fee = 0.002
    ibkr_pricing_mode = "tiered"
    monthly_trade_volume = 0
    reverse = True

    duration = int(input("Enter duration in years: "))
    budget = int(input("Enter budget in USD: "))
    stop_loss = float(input("Enter stop loss in decimals: "))

    time_function(run_price_change_simulation, execution_index, folder, end_date, duration, budget, lot_size, sensitivity, liquidity, stop_loss, max_fee, ibkr_pricing_mode, monthly_trade_volume, reverse)


if __name__ == "__main__":
    main()