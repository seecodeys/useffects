import datetime
import concurrent.futures
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.animation as animation
from subfunctions import *
from functions import *


# Runs a simulation with the provided settings

def run_us_price_change_simulation(execution_index, folder, end_date, duration, budget, lot_size=1, sensitivity=0, liquidity=0.001, stop_loss=float('inf'), max_fee=0.002, ibkr_pricing_mode="tiered", monthly_trade_volume=0, reverse=False):
    # Set initial budget for future reference
    initial_budget = budget

    # Parse the end_date string to a datetime object
    end_date = datetime.datetime.strptime(end_date, "%b %d, %Y")

    # Calculate start date based on the duration (capped at Yahoo Finance Currency Historical Data oldest)
    start_date = max(end_date - pd.Timedelta(days=duration * 365), pd.to_datetime("Dec 1, 2003"))

    # Initiate start date for this instance
    instance_start_date = None

    # Initiate base file name
    base_file_name = f"price_change_{execution_index}_exec_{end_date.strftime('%Y-%m-%d')}_date_{duration}_dura_{initial_budget}_budg_{sensitivity}_sens_{liquidity}_liqu_{stop_loss}_stop_{max_fee}_maxf_{ibkr_pricing_mode}_pmod_{monthly_trade_volume}_motv_{reverse}_reve"

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
        final_df = pd.DataFrame(columns=final_df_columns)
        instance_start_date = start_date
        save_data(final_df, f"{base_file_name}_final_df", "simulations")
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
        log_df_additional_columns += [f'{col} #{i + 1}' for col in ['Prediction', 'Open', 'High', 'Low', 'Close', 'Volume', 'Change', 'Quantity', 'Stop Loss Triggered', 'Profit/Loss'] for i in range(len(execution_symbol_list))]
        log_df_columns += log_df_additional_columns
        log_df = pd.DataFrame(columns=log_df_columns)
        save_data(log_df, f"{base_file_name}_log_df", "simulations")
    else:
        # Set log_df headers based on existing dataframe csv file
        log_df_columns = next(csv.reader(open(f"simulations/{base_file_name}_log_df.csv")))

    # Create execution_index dataframe, filter by start_date or instance_start_date and remove first 10 days (Previous 10D $ Volume)
    execution_index_df = pd.read_csv(f"INDEX/{execution_index}.csv")
    execution_index_initial_budget = None
    execution_index_df['Date'] = pd.to_datetime(execution_index_df["Date"])
    execution_index_df = execution_index_df[execution_index_df["Date"] >= instance_start_date].reset_index(drop=True)
    execution_index_initial_budget = execution_index_df.loc[0, "Open"]
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
        while True and len(current_date_temp_working_df) > 0:
            current_date_temp_working_df['Allocation'] = current_date_temp_working_df['Previous 10D $ Volume'] / current_date_temp_working_df['Previous 10D $ Volume'].sum() * budget
            current_date_temp_working_df['Quantity'] = np.floor(current_date_temp_working_df['Allocation'] / current_date_temp_working_df['Open'])
            if (current_date_temp_working_df['Quantity'] == 0).any():
                current_date_temp_working_df = current_date_temp_working_df.iloc[:-1]
            else:
                break

        # Reoptimize portfolio to eliminate those where fees > max_fees
        while True and len(current_date_temp_working_df) > 0:
            current_date_temp_working_df['Investment'] = current_date_temp_working_df['Quantity'] * current_date_temp_working_df['Open']
            current_date_temp_working_df['Fees'] = current_date_temp_working_df.apply(lambda constituent: 2 * ibkr_us_fees(constituent['Open'], constituent['Quantity'], ibkr_pricing_mode, monthly_trade_volume), axis=1)
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

        # Add the 2 current_date dataframes to the full dataframe csv files in append mode
        current_date_final_df.to_csv(f"simulations/{base_file_name}_final_df.csv", mode='a', header=False, index=False)
        current_date_log_df.to_csv(f"simulations/{base_file_name}_log_df.csv", mode='a', header=False, index=False)

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
    duration = float(20)
    budget = float(25000)
    lot_size = 1
    sensitivity = float(0.02)
    liquidity = 0.000001
    stop_loss = sensitivity / 10
    max_fee = stop_loss
    ibkr_pricing_mode = "tiered"
    monthly_trade_volume = 0
    reverse = True

    duration = float(input("Enter duration in years: "))
    budget = float(input("Enter budget in USD: "))
    sensitivity = float(input("Enter sensitivity in decimals: "))
    stop_loss = float(input("Enter stop loss in decimals: "))

    time_function(run_us_price_change_simulation, execution_index, folder, end_date, duration, budget, lot_size, sensitivity, liquidity, stop_loss, max_fee, ibkr_pricing_mode, monthly_trade_volume, reverse)

if __name__ == "__main__":
    main()