import os
import datetime
import time


# Saves dataframe as a csv
def save_data(df, file, folder, header=True):
    # Create the folder if it doesn't exist
    folder_path = os.path.join(os.getcwd(), folder)
    os.makedirs(folder_path, exist_ok=True)

    # Save the DataFrame as a CSV file
    file_path = os.path.join(folder_path, f"{file}.csv")
    df.to_csv(file_path, index=False, header=header)

    print(
        f"[{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Saved the DataFrame with {len(df)} entries as {file}.csv in the folder {folder}.")


# Times a function runtime
def time_function(func, *args, **kwargs):
    start_time = time.time()  # Record the start time
    func(*args, **kwargs)  # Call the provided function with arguments
    end_time = time.time()  # Record the end time

    # Calculate the runtime
    runtime = end_time - start_time

    # Print the runtime
    print(f"Runtime: {runtime:.4f} seconds")
