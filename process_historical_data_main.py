import multiprocessing

from functions import *

file_path = 'SI/Correct_Symbols.SI.csv'
column_index = 0  # Index of the column to extract
count = 1

# Read the CSV file
with open(file_path, 'r') as file:
    csv_reader = csv.reader(file)

    # Extract values from the first column into a list
    symbols_list = list(set([row[column_index] for row in csv_reader]))


# Define the function to be executed by each process
def process_data(symbol):
    yh_process_historical_data(symbol, "SI", "SGD")


if __name__ == '__main__':
    # Create a pool of processes
    pool = multiprocessing.Pool()

    # Map the fetch_data function to the symbol list
    pool.map(process_data, symbols_list)

    # Close the pool and wait for all processes to finish
    pool.close()
    pool.join()