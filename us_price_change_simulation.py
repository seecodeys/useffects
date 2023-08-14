import datetime
import concurrent.futures
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.animation as animation
from subfunctions import *
from functions import *


# Runs a simulation with the provided settings

def run_us_price_change_simulation(execution_index, folder, end_date, duration, budget, lot_size=1, liquidity=0.00005, stop_loss=0.977, max_fees=0.023, ibkr_pricing_mode="tiered", monthly_trade_volume=0, reverse=False):
    # Set initial budget for future reference
    initial_budget = budget

    # Parse the end_date string to a datetime object
    end_date = datetime.datetime.strptime(end_date, "%b %d, %Y")

    # Calculate start date based on the duration (capped at Yahoo Finance Currency Historical Data oldest)
    start_date = max(end_date - pd.Timedelta(days=duration * 365), pd.to_datetime("Dec 1, 2003"))

    # Initiate start date for this instance
    instance_start_date = None

    # Initiate base file name
    base_file_name = f"dynamic_price_change_{execution_index}_exec_{end_date.strftime('%Y-%m-%d')}_date_{duration}_dura_{initial_budget}_budg_{liquidity}_liqu_{stop_loss}_stop_{max_fees}_maxf_{ibkr_pricing_mode}_pmod_{monthly_trade_volume}_motv_{reverse}_reve"

    # Create list of execution symbols
    execution_symbol_list = []
    for file_name in os.listdir(folder):
        modified_name = file_name.split(".")[0]
        if modified_name not in ["Symbols", "Correct_Symbols", "Rejected_Symbols"]:
            execution_symbol_list.append(modified_name)

    # Initiate simulation headers
    final_df_columns = []
    log_df_columns = []

    # Open / Create simulation final dataframe
    if not os.path.exists(f"simulations/{base_file_name}_final_df.csv"):
        # Create simulation's final dataframe
        final_df_columns = ['Date', 'Starting Amount', 'Invested Amount', 'Remainder', 'Asset Change', 'Fees', 'Profit/Loss', 'Yield', 'Bottom Line']
        instance_start_date = start_date
    else:
        # Open existing dataframe
        final_df = pd.read_csv(f"simulations/{base_file_name}_final_df.csv")
        instance_start_date = pd.to_datetime(final_df['Date'].iloc[-1]) + pd.DateOffset(days=1)
        budget = final_df['Bottom Line'].iloc[-1]
        del final_df

    # Open / Create simulation log dataframe
    if not os.path.exists(f"simulations/{base_file_name}_log_df.csv"):
        # Create simulation's log dataframe according to number of stocks
        log_df_columns = ['Date', 'Portfolio Size', 'Starting Amount', 'Bottom Line']
        log_df_additional_columns = [f'Constituent #{i + 1}' for i in range(len(execution_symbol_list))]
        log_df_additional_columns += [f'{col} #{i + 1}' for col in ['Prediction', 'Open', 'High', 'Low', 'Close', 'Volume', 'Change', 'Quantity', 'Stop Loss', 'Stop Loss Triggered', 'Profit/Loss'] for i in range(len(execution_symbol_list))]
        log_df_columns += log_df_additional_columns
    else:
        # Set log_df headers based on existing dataframe csv file
        log_df_columns = next(csv.reader(open(f"simulations/{base_file_name}_log_df.csv")))

    # Create execution_index dataframe, filter by start_date or instance_start_date and remove first 10 days (Previous 10D $ Volume)
    execution_index_df = pd.read_csv(f"INDEX/{execution_index}.csv")
    execution_index_initial_budget = None
    execution_index_df['Date'] = pd.to_datetime(execution_index_df["Date"])
    execution_index_initial_budget = execution_index_df[execution_index_df["Date"] >= start_date].reset_index(drop=True).loc[0, 'Open']
    execution_index_df = execution_index_df[execution_index_df["Date"] >= instance_start_date].reset_index(drop=True)
    instance_start_date = execution_index_df.loc[0, "Date"]
    if instance_start_date == pd.to_datetime("Dec 1, 2003"):
        execution_index_df = execution_index_df.loc[10:].reset_index(drop=True)
        execution_index_initial_budget = execution_index_df.loc[0, 'Open']
        instance_start_date = execution_index_df.loc[0, "Date"]

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

            # Initiate checks to determine validitty of dataframe - returns True if passed
            na_check = not previous_symbol_entries[['Open', 'High', 'Low', 'Close', 'Volume']].isna().any().any()
            empty_check = len(previous_symbol_entries) == 10
            zero_check = not (previous_symbol_entries[['Open', 'High', 'Low', 'Close']] == 0).any().any()
            ohlc_check = not ((previous_symbol_entries['Open'] == previous_symbol_entries['High']) & (previous_symbol_entries['High'] == previous_symbol_entries['Low']) & (previous_symbol_entries['Low'] == previous_symbol_entries['Close'])).any()

            # Execute following only if no 0 is found in the price columns
            if na_check and empty_check and zero_check and ohlc_check:
                # Assign sensitivity (1 std dev)
                symbol_entry_sensitivity = previous_symbol_entries['% Market Open Change'].loc[previous_symbol_entries['% Market Open Change'].notna()].apply(pd.to_numeric).abs().quantile(q=0.8399)

                # Get previous entry
                previous_symbol_entry = symbol_df.iloc[symbol_entry_index - 1]

                # Execute following only if Previous 10D $ Volume is a number
                if not np.isnan(symbol_entry['Previous 10D $ Volume'].values[0]):
                    # Determine prediction
                    symbol_entry_prediction = None
                    if (symbol_entry['Open'].loc[0] / previous_symbol_entry['Close'] - 1) > symbol_entry_sensitivity and not reverse:
                        symbol_entry_prediction = "H"
                    elif (symbol_entry['Open'].loc[0] / previous_symbol_entry['Close'] - 1) > symbol_entry_sensitivity and reverse:
                        symbol_entry_prediction = "L"
                    elif (1 - symbol_entry['Open'].loc[0] / previous_symbol_entry['Close']) > symbol_entry_sensitivity and not reverse:
                        symbol_entry_prediction = "L"
                    elif (1 - symbol_entry['Open'].loc[0] / previous_symbol_entry['Close']) > symbol_entry_sensitivity and reverse:
                        symbol_entry_prediction = "H"

                    # Execute following only when there is a prediction
                    if symbol_entry_prediction:
                        # Calculate Stop Loss
                        # Initiate conditions to determine day change
                        day_change_conditions = [
                            previous_symbol_entries['Close'] / previous_symbol_entries['Open'] - 1 > 0,
                            previous_symbol_entries['Close'] / previous_symbol_entries['Open'] - 1 < 0
                        ]
                        # Initiate Maximum Opposite Change values
                        max_opposite_change_values = [
                            np.select(day_change_conditions, [1 - previous_symbol_entries['Low'] / previous_symbol_entries['Open'], 0], default=0),
                            np.select(day_change_conditions, [0, previous_symbol_entries['High'] / previous_symbol_entries['Open'] - 1], default=0)
                        ]
                        # Use 'assign()' method to update 'Max Opposite Change' and 'Day Move' columns in one line
                        previous_symbol_entries = previous_symbol_entries.assign(
                            **{
                                'Max Opposite Change': np.select(day_change_conditions, max_opposite_change_values, default=0)
                            }
                        )
                        # Assign stop loss
                        symbol_entry_stop_loss = previous_symbol_entries['Max Opposite Change'].quantile(q=stop_loss)

                        # Assign max fees
                        symbol_entry_max_fees = abs(previous_symbol_entries['Close'] / previous_symbol_entries['Open'] - 1).quantile(q=max_fees)

                        # Get maximum quantity based on liquidity
                        symbol_entry_max_qty = np.floor((previous_symbol_entries['$ Volume'].min() * liquidity) / lot_size) * lot_size
                        # Add entry only when symbol_entry_max_qty > lot_size
                        if symbol_entry_max_qty != 0 and symbol_entry_max_fees > 0:
                            # Leave necessary columns and format them
                            symbol_entry = symbol_entry.drop(columns=['% Day Change', '% Previous Change', '% Market Open Change', '$ Volume'])
                            symbol_entry_dv = symbol_entry['Previous 10D $ Volume']
                            symbol_entry.drop('Previous 10D $ Volume', axis=1, inplace=True)
                            symbol_entry.insert(1, 'Symbol', [symbol])
                            symbol_entry.insert(2, 'Previous 10D $ Volume', symbol_entry_dv)
                            symbol_entry.insert(3, 'Prediction', symbol_entry_prediction)
                            symbol_entry.insert(4, 'Stop Loss', symbol_entry_stop_loss)
                            symbol_entry.insert(5, 'Max Fees', symbol_entry_max_fees)

                            # Return symbol_entry to add to current_date_final_df and current_date_log_df
                            # **
                            print(symbol_entry)
                            return symbol_entry

    # Create function to process each day
    def process_date(current_date):
        # Import nonlocal variables
        nonlocal budget

        # Create current_date_temp_working_df, current_date_log_df and current_date_final_df
        current_date_temp_working_df = pd.DataFrame(columns=['Date', 'Symbol', 'Previous 10D $ Volume', 'Prediction', 'Stop Loss', 'Max Fees', 'Open', 'High', 'Low', 'Close', 'Volume'])
        current_date_log_df = pd.DataFrame(columns=log_df_columns)
        current_date_final_df = pd.DataFrame(columns=final_df_columns)

        # # Initiate multithreading
        # with concurrent.futures.ThreadPoolExecutor() as executor:
        #     # Submit symbol processing tasks to the executor
        #     futures = [executor.submit(process_symbol, symbol, current_date) for symbol in execution_symbol_list]
        #
        #     # Wait for all threads to finish
        #     concurrent.futures.wait(futures, return_when=concurrent.futures.ALL_COMPLETED)
        #
        #     # Retrieve results from completed tasks
        #     for future in concurrent.futures.as_completed(futures):
        #         symbol_entry = future.result()
        #         current_date_temp_working_df = pd.concat([current_date_temp_working_df, symbol_entry])
        #
        # # Sort current_date_temp_working_df according to Previous 10D $ Volume
        # current_date_temp_working_df = current_date_temp_working_df.sort_values(by='Previous 10D $ Volume', ascending=False).reset_index(drop=True)
        # **
        current_date_temp_working_df = pd.read_csv('C:\\Users\\User\\Coding\\Trading\\useffects\\INDEX\\TEST.csv')
        # Equal Weight (EW)
        # Reoptimize portfolio to eliminate those where allocation > max allocation and where fees > max_fees (where max_fees = stop_loss / 10)
        while True and len(current_date_temp_working_df) > 0:
            current_date_temp_working_df['Max Allocation'] = current_date_temp_working_df['Previous 10D $ Volume'] * liquidity
            current_date_temp_working_df['Allocation'] = budget / len(current_date_temp_working_df)
            current_date_temp_working_df['Quantity'] = np.floor(current_date_temp_working_df['Allocation'] / current_date_temp_working_df['Open'])
            current_date_temp_working_df['Investment'] = current_date_temp_working_df['Quantity'] * current_date_temp_working_df['Open']
            current_date_temp_working_df['Fees'] = current_date_temp_working_df.apply(lambda constituent: 2 * ibkr_us_fees(constituent['Open'], constituent['Quantity'], ibkr_pricing_mode, monthly_trade_volume), axis=1)
            current_date_temp_working_df['Fee Percentage'] = current_date_temp_working_df['Fees'] / current_date_temp_working_df['Investment']
            if (current_date_temp_working_df['Allocation'] > current_date_temp_working_df['Max Allocation']).any():
                current_date_temp_working_df = current_date_temp_working_df.iloc[:-1]
                print("Reduced Portfolio Size - Max Allocation")
                # print(current_date_temp_working_df)
            elif  (current_date_temp_working_df['Quantity'] == 0).any():
                current_date_temp_working_df = current_date_temp_working_df.iloc[:-1]
                print("Reduced Portfolio Size - Quantity")
                # print(current_date_temp_working_df)
            # elif  (current_date_temp_working_df['Fee Percentage'] > current_date_temp_working_df['Max Fees']).any():
            #     current_date_temp_working_df = current_date_temp_working_df.iloc[:-1]
            #     print("Reduced Portfolio Size - Fee Percentage")
            #     # print(current_date_temp_working_df)
            else:
                current_date_temp_working_df.drop('Max Allocation', axis=1, inplace=True)
                # print(current_date_temp_working_df)
                save_data(current_date_temp_working_df, "TEST_EDITED", "index")
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
            # Assign prediction and stop loss for entry
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
            current_date_log_df.loc[0, f"Stop Loss #{index + 1}"] = entry['Stop Loss']
            # Assign asset change to Profit/Loss
            if entry_prediction == "H":
                # Implement stop losses
                if 1 - entry['Low'] / entry['Open'] > entry['Stop Loss'] and stop_loss:
                    current_date_log_df.loc[0, f"Profit/Loss #{index + 1}"] = (entry['Open'] * entry['Stop Loss']) * entry['Quantity'] * -1
                    current_date_log_df.loc[0, f"Stop Loss Triggered #{index + 1}"] = True
                    stop_loss_count += 1
                else:
                    current_date_log_df.loc[0, f"Profit/Loss #{index + 1}"] = (entry['Close'] - entry['Open']) * entry['Quantity']
                    current_date_log_df.loc[0, f"Stop Loss Triggered #{index + 1}"] = False
            elif entry_prediction == "L":
                # Implement stop losses
                if entry['High'] / entry['Open'] - 1 > entry['Stop Loss'] and stop_loss:
                    current_date_log_df.loc[0, f"Profit/Loss #{index + 1}"] = (entry['Open'] * entry['Stop Loss']) * entry['Quantity'] * -1
                    current_date_log_df.loc[0, f"Stop Loss Triggered #{index + 1}"] = True
                    stop_loss_count += 1
                else:
                    current_date_log_df.loc[0, f"Profit/Loss #{index + 1}"] = (entry['Open'] - entry['Close']) * entry['Quantity']
                    current_date_log_df.loc[0, f"Stop Loss Triggered #{index + 1}"] = False
            # Deduct fees from Profit/Loss
            current_date_log_df.loc[0, f"Profit/Loss #{index + 1}"] -= ibkr_us_fees(entry['Open'], entry['Quantity'], ibkr_pricing_mode, monthly_trade_volume)
            # Implement stop losses
            if entry_prediction == "H" and 1 - entry['Low'] / entry['Open'] >= entry['Stop Loss']:
                current_date_log_df.loc[0, f"Profit/Loss #{index + 1}"] -= ibkr_us_fees(entry['Open'] * (1 - entry['Stop Loss']), entry['Quantity'], ibkr_pricing_mode, monthly_trade_volume)
            # Implement stop losses
            elif entry_prediction == "L" and entry['High'] / entry['Open'] >= entry['Stop Loss']:
                current_date_log_df.loc[0, f"Profit/Loss #{index + 1}"] -= ibkr_us_fees(entry['Open'] * (1 + entry['Stop Loss']), entry['Quantity'], ibkr_pricing_mode, monthly_trade_volume)
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

        # Add the 2 current_date dataframes to the full dataframe csv files in append mode
        output_file_final_df = f"simulations/{base_file_name}_final_df.csv"
        output_file_log_df = f"simulations/{base_file_name}_log_df.csv"

        # Check if the output files already exist
        file_exists_final_df = os.path.exists(output_file_final_df)
        file_exists_log_df = os.path.exists(output_file_log_df)

        # Write data to CSV files without headers if they don't exist
        current_date_final_df.to_csv(output_file_final_df, mode='a', header=not file_exists_final_df, index=False)
        current_date_log_df.to_csv(output_file_log_df, mode='a', header=not file_exists_log_df, index=False)

        # Set budget to bottom line
        budget = max(current_date_final_df.loc[0, f"Bottom Line"], 0)

        # Initiate status update variables
        daily_yield = current_date_final_df.loc[0, 'Yield']
        balance = current_date_final_df.loc[0, 'Bottom Line']
        average_return = 0
        execution_index_average_return = 0
        years = ((current_date - start_date).days / 365)
        execution_index_close = execution_index_df.loc[execution_index_df['Date'] == current_date, 'Close'].values[0]
        if current_date > start_date:
            average_return = ((balance / initial_budget) ** (1 / years)) - 1
            execution_index_average_return = ((execution_index_close / execution_index_initial_budget) ** (1 / years)) - 1

        # Format status update variables
        daily_yield = f"{'%.2f' % (round(daily_yield, 4) * 100)}%"
        balance = f"${'%.2f' % (round(balance, 2))}"
        stopped_out = f"{stop_loss_count}/{len(current_date_temp_working_df)}"
        average_return = f"{'%.2f' % (round(average_return, 4) * 100)}%"
        execution_index_average_return = f"{'%.2f' % (round(execution_index_average_return, 4) * 100)}%"

        # Print status update
        print(f"[{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {current_date.strftime('%Y-%m-%d')} Daily Yield: {daily_yield} | Balance: {balance } | {stopped_out} Stopped Out | Average Return: {average_return} | Execution Index Average Return: {execution_index_average_return}")

    for date in execution_index_df['Date']:
        # Run process_date for current date
        process_date(date)
        break

        # if date == pd.to_datetime("2003/12/30", format="%Y/%m/%d"):
        #     print(date)
        #     # Run process_date for current date
        #     process_date(date)
    #
    #     # # Graph cumulative final_df performance
    #     # final_df_x = [final_df.iloc[0, 0]]
    #     # final_df_y = [initial_budget]
    #     # final_df_x.extend(final_df['Date'])
    #     # final_df_y.extend(final_df['Bottom Line'])


def main():
    execution_index = "%5EGSPC"
    folder = "US"
    end_date = "Jul 15, 2023"
    duration = float(5)
    budget = float(2500)
    lot_size = 1
    liquidity = 0.00005
    stop_loss = 0.023
    max_fees = 0.5
    ibkr_pricing_mode = "tiered"
    monthly_trade_volume = 0
    reverse = True

    # duration = float(input("Enter duration in years: "))
    # budget = float(input("Enter budget in USD: "))
    # stop_loss = float(input("Enter stop loss percentile in decimals: "))

    time_function(run_us_price_change_simulation, execution_index, folder, end_date, duration, budget, lot_size, liquidity, stop_loss, max_fees, ibkr_pricing_mode, monthly_trade_volume, reverse)

if __name__ == "__main__":
    main()