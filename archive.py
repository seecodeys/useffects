import concurrent
import re
import pandas as pd
import requests
from bs4 import BeautifulSoup
from jukes import *

from subfunctions import *

# Fetches historical data from Yahoo Finance

def yh_fetch_historical_data_archived(code, end_date, duration, folder):
    fetch_count = 1

    # Parse the end_date string to a datetime object
    end_date = datetime.datetime.strptime(end_date, "%b %d, %Y")

    # Calculate start date based on the duration (capped at Yahoo Finance Currency Historical Data oldest)
    start_date = max(end_date - datetime.timedelta(days=duration * 365), datetime.datetime.strptime("Dec 1, 2003", "%b %d, %Y"))

    # Convert start and end dates to epoch time
    start_date_epoch = int(start_date.timestamp())
    end_date_epoch = int(end_date.timestamp())

    headers = generate_header()
    proxies = generate_proxy()

    # Initialize an empty DataFrame
    df = pd.DataFrame()

    previous_data = None

    while True:
        print(f"[{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {code}: Fetch #{fetch_count} Started...")

        # Format the URL with the symbol, exchange, and epoch time
        url = f"https://finance.yahoo.com/quote/{code}/history?period1={start_date_epoch}&period2={end_date_epoch}&interval=1d&filter=history&frequency=1d&includeAdjustedClose=true"

        response = requests.get(url, headers=headers, proxies=proxies)
        soup = BeautifulSoup(response.text, "html.parser")

        span = soup.select_one("#quote-header-info > div > div > div > span").text.strip()
        pattern = r'Currency in (\w+)'
        match = re.search(pattern, span)
        currency = match.group(1)

        table = soup.find("table")
        data_rows = table.find("tbody").find_all("tr")

        column_names = [th.get_text() for th in table.find_all("th")]
        data = []

        for row in data_rows:
            row_data = [td.get_text() for td in row.find_all("td")]
            if len(row_data) < 7:
                continue
            data.append(row_data[:5] + row_data[6:])  # Exclude the "Adj Close" column

        new_df = pd.DataFrame(data, columns=column_names[:5] + column_names[6:])  # Exclude the "Adj Close" column name

        # Replace column names
        new_df.rename(columns={"Close*": "Close"}, inplace=True)

        # Remove commas from numeric columns (excluding Date)
        numeric_columns = new_df.columns.drop(["Date"])
        new_df[numeric_columns] = new_df[numeric_columns].replace(",", "", regex=True).replace("-", 0, regex=True)

        new_df["Date"] = pd.to_datetime(new_df["Date"], format="%b %d, %Y")
        new_df["Open"] = pd.to_numeric(new_df["Open"])
        new_df["High"] = pd.to_numeric(new_df["High"])
        new_df["Low"] = pd.to_numeric(new_df["Low"])
        new_df["Close"] = pd.to_numeric(new_df["Close"])
        new_df["Volume"] = pd.to_numeric(new_df["Volume"])

        # Remove entries with 0 values in Open, Close, High, and Low
        new_df = new_df[(new_df["Open"] != 0) & (new_df["Close"] != 0) & (new_df["High"] != 0) & (new_df["Low"] != 0)]

        # Check if the new data is identical to the previous data
        if previous_data is not None and new_df.equals(previous_data):
            break

        # Append new data to the existing DataFrame
        df = pd.concat([df, new_df], sort=False)

        # Update the end_date_epoch with the smallest date from the new data (assuming error only caused by last fetch)
        if not pd.isnull(new_df["Date"].min()):
            end_date_epoch = int(new_df["Date"].min().timestamp())
        else:
            break

        # Break the loop if the smallest date is smaller than or equal to the start_date
        if new_df["Date"].min() <= start_date:
            break

        fetch_count += 1
        previous_data = new_df.copy()

    # Add currency column
    df["Currency"] = currency

    # Remove duplicates from the DataFrame
    df = df.drop_duplicates()

    # Sort final dataframe according to date
    df = df.sort_values("Date", ascending=True).reset_index(drop=True)

    save_data(df, f"{code}", folder, True)

# Fetches historical data from MarketWatch

def mw_fetch_historical_data_archived(type, symbol, end_date, duration, folder, country_code="", chunk_size=30):
    # Parse the end_date string to a datetime object
    end_date = datetime.datetime.strptime(end_date, "%b %d, %Y")

    # Calculate the start date based on the duration
    start_date = end_date - datetime.timedelta(days=duration * 365)

    # Initialize an empty dataframe to store the historical data
    df = pd.DataFrame()

    # Fetch data in smaller date ranges
    while end_date >= start_date:
        # Calculate the start and end dates for the current chunk
        chunk_end_date = end_date
        chunk_start_date = end_date - datetime.timedelta(days=chunk_size - 1)
        if chunk_start_date < start_date:
            chunk_start_date = start_date

        # Format the start_date and end_date strings
        chunk_start_date_str = chunk_start_date.strftime("%#m/%#d/%Y")
        chunk_end_date_str = chunk_end_date.strftime("%#m/%#d/%Y")

        # Format the URL with the symbol and dates
        url = f"https://www.marketwatch.com/investing/{type}/{symbol}/downloaddatapartial?partial=true&index=0&countryCode={country_code}&iso=&startDate={chunk_start_date_str}&endDate={chunk_end_date_str}&frequency=null&downloadPartial=false&csvDownload=false&newDates=true"
        # Set the User-Agent header to simulate a request from a device
        headers = generate_header()

        # Send a GET request to the URL with headers
        response = requests.get(url, headers=headers)

        # Parse the response content using BeautifulSoup
        soup = BeautifulSoup(response.content, "html.parser")

        # Find the correct table element containing the historical data
        table = soup.select_one("#download-data-tabs > div > div.overflow--table > table")

        # Extract data rows from the table body
        data_rows = table.find_all("tr")[1:]  # Exclude the header row

        # Extract data from each row and store it in the dataframe
        for row in data_rows:
            row_data = [td.text for td in row.find_all("td")]
            if len(row_data) >= 5:
                row_dict = {
                    "Date": row_data[0],
                    "Open": row_data[1].replace("$", "").replace(",", ""),
                    "High": row_data[2].replace("$", "").replace(",", ""),
                    "Low": row_data[3].replace("$", "").replace(",", ""),
                    "Close": row_data[4].replace("$", "").replace(",", "")
                }
                df = df.append(row_dict, ignore_index=True)

        # Update the end_date for the next chunk
        end_date = chunk_start_date - datetime.timedelta(days=1)

    # Change the order of columns
    df = df[['Date', 'Open', 'High', 'Low', 'Close']]

    # Convert numeric columns to appropriate data types
    df["Open"] = pd.to_numeric(df["Open"])
    df["High"] = pd.to_numeric(df["High"])
    df["Low"] = pd.to_numeric(df["Low"])
    df["Close"] = pd.to_numeric(df["Close"])
    df['Date'] = df['Date'].str.strip()
    df['Date'] = df['Date'].str.split('\n').str[0]
    df['Date'] = pd.to_datetime(df['Date'], format='%m/%d/%Y')

    # Add Previous Change
    df['% Previous Change'] = round(df['Close'] / df['Close'].shift(1) - 1, 4)

    df = df.sort_values('Date', ascending=True)
    save_data(df, symbol, folder, True)

# Round all $V in all stocks for an exchange to 2 decimal places
def remove_decimals_archived(exchange):
    earliest_date = datetime.datetime.strptime("Dec 1, 2003", "%b %d, %Y")
    file_path = f"{exchange}/Correct_Symbols.{exchange}.csv"
    column_index = 0  # Index of the column to extract

    # Read the CSV file
    with open(file_path, 'r') as file:
        csv_reader = csv.reader(file)

        # Extract values from the first column into a list
        symbols_list = list(set([row[column_index] for row in csv_reader]))
        # Remove blank elements
        symbols_list = [value for value in symbols_list if value]

    def process_symbol(symbol):
        csv_file = f"{exchange}/{symbol}.{exchange}.csv"
        df = pd.read_csv(csv_file)
        df["50D $ Volume"] = round(df["50D $ Volume"] , 2)
        df["$ Volume"] = round(df["$ Volume"], 2)

        # Save the modified DataFrame back to the original CSV file
        df.to_csv(csv_file, index=False)
        print(f"[{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {symbol}: Completed")

    with concurrent.futures.ThreadPoolExecutor() as executor:
        executor.map(process_symbol, symbols_list)

# Fetches all stocks information from eoddata.com

def eod_fetch_stock_data_archived(exchange, folder):
    run_instance = 1
    pages_list = [1]

    headers = generate_header()

    # Initialize an empty DataFrame
    df = pd.DataFrame()

    while True:
        print(f"[{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {exchange}: Fetch #{run_instance} Started...")
        url = f"https://eoddata.com/stocklist/{exchange}/{pages_list[run_instance - 1]}.htm"

        # Send an HTTP GET request to the website
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            # Parse the HTML content using BeautifulSoup
            soup = BeautifulSoup(response.content, "html.parser")

            tables = soup.find_all("table")

            if len(tables) >= 6:
                table = tables[5]
                data_rows = table.find_all("tr")
                if run_instance == 1:
                    for letter in tables[4].find_all("a"):
                        pages_list.append(letter.text.strip())

                # Create an empty list to store the extracted data
                data = []

                # Extract the data from each row and store it in the list
                for row in data_rows:
                    row_data = [cell.text.strip() for cell in row.find_all("td")]
                    data.append(row_data)

                # Create a DataFrame from the extracted data
                column_names = ["Symbol", "Name", "High", "Low", "Close", "Volume", "Change ($)", "", "Change (%)", ""]
                new_df = pd.DataFrame(data, columns=column_names)
                new_df.drop(['', ''], axis=1, inplace=True)
                new_df.drop([0], inplace=True)

                # Replace commas in the "Volume" column and convert it to a number
                new_df["Volume"] = new_df["Volume"].str.replace(",", "").astype(int)

                # Append new data to the existing DataFrame
                df = pd.concat([df, new_df])
            else:
                print("Table not found on the page.")
        else:
            print("Failed to retrieve website content. Status code:", response.status_code)

        run_instance += 1

        if len(pages_list) == run_instance:
            break

    save_data(df, f"Symbols.{folder}", folder, True)

# Generates a random proxy address * DOESNT WORK

def generate_proxy_archive():
    url = "https://free-proxy-list.net/"

    while True:
        try:
            response = requests.get(url)
            soup = BeautifulSoup(response.content, "html.parser")
            table_rows = soup.select_one("#list > div > div.table-responsive > div > table").find_all("tr")
            # Extract the header row
            header_row = table_rows[0]
            header_cells = header_row.find_all("th")
            header_data = [cell.text for cell in header_cells]

            # Extract the data rows
            data_rows = table_rows[1:]
            data = []
            for row in data_rows:
                cells = row.find_all("td")
                row_data = [cell.text for cell in cells]
                data.append(row_data)

            # Create dataframe
            df = pd.DataFrame(data, columns=header_data)

            # Filter dataframe
            df = df[(df['Anonymity'] == "elite proxy") & (df['Https'] == "yes")]
            df.reset_index(drop=True, inplace=True)
            random_proxy = f"{df.sample().iloc[0]['IP Address']}:{df.sample().iloc[0]['Port']}"

            return random_proxy

        except (requests.exceptions.RequestException, KeyError):
            pass

# Multithread to fetch historical data from Yahoo Finance

def yh_process_chunk(start_date, end_date, code, folder, lock):
    # Convert start and end dates to epoch time (GMT timezone)
    start_date_epoch = int(start_date.timestamp())
    end_date_epoch = int(end_date.timestamp())

    headers = generate_header()

    # Format the URL with the symbol, exchange, and epoch time
    url = f"https://finance.yahoo.com/quote/{code}/history?period1={start_date_epoch}&period2={end_date_epoch}&interval=1d&filter=history&frequency=1d&includeAdjustedClose=true"

    response = requests.get(url, headers=headers)
    soup = BeautifulSoup(response.text, "html.parser")

    span = soup.select_one("#quote-header-info > div > div > div > span").text.strip()
    pattern = r'Currency in (\w+)'
    match = re.search(pattern, span)
    currency = match.group(1)

    table = soup.find("table")
    data_rows = table.find("tbody").find_all("tr")

    column_names = [th.get_text() for th in table.find_all("th")]
    data = []

    # Initiate list that tracks unaccounted stock split
    split_list = []

    for index, row in enumerate(data_rows):
        row_data = [td.get_text() for td in row.find_all("td")]
        if len(row_data) < 7:
            if row_data[1].find("Stock Split") != -1:
                split_ratio = re.findall(r'\d+', row_data[1])
                start_count = int(split_ratio[0])
                end_count = int(split_ratio[1])

                # Loops to find previous/next entries that aren't dividends/splits
                previous_entry = next(([td.get_text() for td in row.find_all("td")] for row in data_rows[index + 1:] if
                                       len([td.get_text() for td in row.find_all("td")]) == 7), [])
                next_entry = next(
                    ([td.get_text() for td in data_rows[i].find_all("td")] for i in range(index - 1, -1, -1) if
                     len([td.get_text() for td in data_rows[i].find_all("td")]) == 7), None)

                # Finds if the difference is suspicious using a threshold
                threshold = 0.1
                previous_close = float(previous_entry[4])
                next_close = float(next_entry[4])
                percentage_diff = abs(previous_close / next_close)

                # Verify the stock split adjustment and add to the list
                if percentage_diff <= 1 - threshold or percentage_diff >= 1 + threshold:
                    split_list.append([row_data[0], end_count / start_count])

            continue
        data.append(row_data[:5] + row_data[6:])  # Exclude the "Adj Close" column

    new_df = pd.DataFrame(data, columns=column_names[:5] + column_names[6:])  # Exclude the "Adj Close" column name

    # Replace column names
    new_df.rename(columns={"Close*": "Close"}, inplace=True)

    # Remove commas from numeric columns (excluding Date)
    numeric_columns = new_df.columns.drop(["Date"])
    new_df[numeric_columns] = new_df[numeric_columns].replace(",", "", regex=True).replace("-", 0, regex=True)

    new_df["Date"] = pd.to_datetime(new_df["Date"], format="%b %d, %Y")
    new_df["Open"] = pd.to_numeric(new_df["Open"])
    new_df["High"] = pd.to_numeric(new_df["High"])
    new_df["Low"] = pd.to_numeric(new_df["Low"])
    new_df["Close"] = pd.to_numeric(new_df["Close"])
    new_df["Volume"] = pd.to_numeric(new_df["Volume"])

    # Remove entries with 0 values in Open, Close, High, and Low
    new_df = new_df[(new_df["Open"] != 0) & (new_df["Close"] != 0) & (new_df["High"] != 0) & (new_df["Low"] != 0)]

    with lock:
        csv_file = f"{folder}/{code}.csv"
        if os.path.isfile(csv_file):
            existing_df = pd.read_csv(csv_file)
            df = pd.concat([existing_df, new_df], sort=False).reset_index(drop=True)
        else:
            df = new_df
        df.drop_duplicates(subset=None, keep="first", inplace=True)
        df.to_csv(csv_file, index=False)

    print(f"[{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {code}: Chunk {start_date.strftime('%d-%m-%Y')} to {end_date.strftime('%d-%m-%Y')} Complete")
    return currency, split_list


# Fetches historical data from Yahoo Finance

def yh_fetch_historical_data_archived_2(code, end_date, duration, folder):
    # Parse the end_date string to a datetime object
    end_date = datetime.datetime.strptime(end_date, "%b %d, %Y")

    # Calculate start date based on the duration (capped at Yahoo Finance Currency Historical Data oldest)
    start_date = max(end_date - datetime.timedelta(days=duration * 365),
                     datetime.datetime.strptime("Dec 1, 2003", "%b %d, %Y"))

    # Create date ranges for fetching data in smaller chunks (99 days per chunk)
    date_ranges = []
    while end_date >= start_date:
        date_ranges.append((start_date, min(start_date + datetime.timedelta(days=99), end_date)))
        start_date += datetime.timedelta(days=100)

    lock = threading.Lock()
    currencies = []
    shared_split_list = []

    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = []
        for start, end in date_ranges:
            futures.append(executor.submit(yh_process_chunk, start, end, code, folder, lock))

        for future in concurrent.futures.as_completed(futures):
            result = future.result()
            currencies.append(result[0])
            shared_split_list.extend(result[1])

    # Get the unique currency from the first chunk (assuming currency remains the same for all chunks)
    currency = currencies[0]

    # Read the target CSV file
    csv_file = f"{folder}/{code}.csv"
    df = pd.read_csv(csv_file)

    # Convert Dates in .csv to datetime object
    df['Date'] = pd.to_datetime(df["Date"], format="%Y-%m-%d")

    # Add currency column
    df["Currency"] = currency

    # Remove duplicates from the DataFrame
    df.drop_duplicates(inplace=True)

    # Sort final dataframe according to date
    df.sort_values("Date", inplace=True)
    df.reset_index(drop=True, inplace=True)

    # Apply unaccounted for splits
    for date, multiplier in shared_split_list:
        # Convert the date to datetime object
        date = pd.to_datetime(date, format="%b %d, %Y")

        # Filter the dataframe based on the date condition
        mask = df['Date'] < date
        df.loc[mask, ['Open', 'High', 'Low', 'Close']] /= multiplier

    save_data(df, code, folder, True)