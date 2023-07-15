import os
import multiprocessing
from functions import *
from subfunctions import *

base_currency = "SGD"
eod_exchange = "SGX"
yh_exchange = "SI"
end_date = "Jul 12, 2023"
duration = 20
file_path = f"{yh_exchange}/Symbols.{yh_exchange}.csv"
correct_file_path = f"{yh_exchange}/Correct_Symbols.{yh_exchange}.csv"
column_index = 0  # Index of the column to extract
execution_index = "%5ESTI"
reference_index = "%5EGSPC"

# Fetch EOD stock data
time_function(eod_fetch_stock_data, eod_exchange, yh_exchange)

# Extract symbols from CSV file made from eod_fetch_stock_data()
with open(file_path, 'r') as file:
    csv_reader = csv.reader(file)
    symbols_list = list(set(row[column_index] for row in csv_reader))

# Create Corrected_Symbols.{yh_exchange}.csv
time_function(test_historical_data, symbols_list, yh_exchange)

# Extract symbols from corrected CSV file made from test_historical_data()
with open(correct_file_path, 'r') as file:
    csv_reader = csv.reader(file)
    symbols_list = list(set(row[column_index] for row in csv_reader))

    # Iterate over the file names and modify them
    # for file_name in os.listdir(yh_exchange):
    #     modified_name = file_name.replace(f".{yh_exchange}.csv", "")
    #     if modified_name not in ["Symbols", "Correct_Symbols", "Rejected_Symbols"]:
    #         symbols_list.remove(modified_name)

# Fetch historical data
def fetch_data(symbol):
    time_function(yh_fetch_historical_data, f"{symbol}.{yh_exchange}", end_date, duration, yh_exchange)

# Process historical data
def process_data(symbol):
    time_function(yh_process_historical_data, f"{symbol}.{yh_exchange}", yh_exchange, base_currency)

if __name__ == '__main__':
    multiprocessing.freeze_support()

    # Create a pool of processes
    with multiprocessing.Pool() as pool:
        # Fetch historical data for all symbols
        pool.map(fetch_data, symbols_list)

    # Remove blank historical data
    time_function(remove_blank_historical_data, yh_exchange)

    # Search FX data
    time_function(search_fx_data, yh_exchange, base_currency)

    # Create a pool of processes
    with multiprocessing.Pool() as pool:
        # Process historical data for all symbols
        pool.map(process_data, symbols_list)

    # Fetches index data
    time_function(yh_fetch_historical_data, execution_index, end_date, duration, "index")
    time_function(yh_fetch_historical_data, reference_index, end_date, duration, "index")

    # Processes index data
    time_function(yh_process_historical_data, execution_index, "index", base_currency)
    time_function(yh_process_historical_data, reference_index, "index", base_currency)