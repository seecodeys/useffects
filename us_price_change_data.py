import os
import multiprocessing
from functions import *
from subfunctions import *

base_currency = "USD"
folder = "US"
security_type = "stocks"
end_date = "Jul 12, 2023"
duration = 20
file_path = f"{folder}/Symbols.{folder}.csv"
correct_file_path = f"{folder}/Correct_Symbols.{folder}.csv"
column_index = 0  # Index of the column to extract
execution_index = "%5EGSPC"

# # Fetch all stock symbols
# time_function(mw_fetch_security_list, security_type)
#
# # Format US stock symbols
# time_function(mw_format_yh_us_stocks)

# Extract symbols from CSV file made from eod_fetch_stock_data()
with open(file_path, 'r') as file:
    csv_reader = csv.reader(file)
    symbols_list = list(set(row[column_index] for row in csv_reader))

# Create Corrected_Symbols.{yh_exchange}.csv
time_function(test_historical_data, symbols_list, folder)
#
# # Extract symbols from corrected CSV file made from test_historical_data()
# with open(correct_file_path, 'r') as file:
#     csv_reader = csv.reader(file)
#     symbols_list = list(set(row[column_index] for row in csv_reader))
#
#     # Iterate over the file names and modify them
#     # for file_name in os.listdir(yh_exchange):
#     #     modified_name = file_name.replace(f".{yh_exchange}.csv", "")
#     #     if modified_name not in ["Symbols", "Correct_Symbols", "Rejected_Symbols"]:
#     #         symbols_list.remove(modified_name)
#
# # Fetch historical data
# def fetch_data(symbol):
#     time_function(yh_fetch_historical_data, symbol, end_date, duration, eod_exchange)
#
# # Process historical data
# def process_data(symbol):
#     time_function(yh_process_historical_data, symbol, eod_exchange, base_currency)
#
# if __name__ == '__main__':
#     multiprocessing.freeze_support()
#
#     # Create a pool of processes
#     with multiprocessing.Pool() as pool:
#         # Fetch historical data for all symbols
#         pool.map(fetch_data, symbols_list)
#
#     # Remove blank historical data
#     time_function(remove_blank_historical_data, yh_exchange)
#
#     # Search FX data
#     time_function(search_fx_data, yh_exchange, base_currency)
#
#     # Create a pool of processes
#     with multiprocessing.Pool() as pool:
#         # Process historical data for all symbols
#         pool.map(process_data, symbols_list)
#
#     # Fetches index data
#     time_function(yh_fetch_historical_data, execution_index, end_date, duration, "index")
#
#     # Processes index data
#     time_function(yh_process_historical_data, execution_index, "index", base_currency)