from functions import *
import csv
import datetime
import multiprocessing
import os

file_path = 'SI/Correct_Symbols.SI.csv'
column_index = 0  # Index of the column to extract
count = 1

# Read the CSV file
with open(file_path, 'r') as file:
    csv_reader = csv.reader(file)

    # Extract values from the first column into a list
    symbols_list = list(set([row[column_index] for row in csv_reader]))
    folder_path = "SI"

    # Get all file names in the folder
    file_names = os.listdir(folder_path)

    # Remove ".SI" from the file names
    cleaned_names = [name.replace(".SI.csv", "") for name in file_names]

    # Find symbols that are not in the folder
    missing_symbols = [symbol for symbol in symbols_list if symbol not in cleaned_names]


# Define the function to be executed by each process
def fetch_data(symbol):
    yh_fetch_historical_data(f"{symbol}.SI", "Jul 07, 2023", 20, "SI")


if __name__ == '__main__':
    # Create a pool of processes
    pool = multiprocessing.Pool()

    # Map the fetch_data function to the symbol list
    pool.map(fetch_data, missing_symbols)

    # Close the pool and wait for all processes to finish
    pool.close()
    pool.join()
