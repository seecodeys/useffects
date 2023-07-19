import csv
import threading
import json
import pandas
import requests
from bs4 import BeautifulSoup
import pandas as pd
import datetime
from subfunctions import *
import time
import concurrent.futures
import re
import numpy as np
from jukes import *
from dateutil.relativedelta import relativedelta


# Fetches historical data from Yahoo Finance

def yh_fetch_historical_data(code, end_date, duration, folder, interval="1d", pre_post=False):
    # Parse the end_date string to a datetime object
    end_date = datetime.datetime.strptime(end_date, "%b %d, %Y")

    # Calculate start date based on the duration (capped at Yahoo Finance Currency Historical Data oldest)
    start_date = max(end_date - datetime.timedelta(days=duration * 365),
                     datetime.datetime.strptime("Dec 1, 2003", "%b %d, %Y"))

    # Convert start and end dates to epoch time (GMT timezone)
    start_date_epoch = int(start_date.timestamp())
    end_date_epoch = int(end_date.timestamp())

    # Convert arguments to URL format
    if pre_post == True:
        pre_post = "true"
    elif pre_post == False:
        pre_post = "false"

    # Format the URL
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{code}?symbol={code}&period1={start_date_epoch}&period2={end_date_epoch}&useYfid=true&interval={interval}&includePrePost={pre_post}&events=div%7Csplit%7Cearn&lang=en-US&region=US&crumb=eREX9CqAe3K&corsDomain=finance.yahoo.com"

    # Generate headers
    headers = generate_header()

    # Run a GET request
    response = requests.get(url, headers=headers)

    # Extract the relevant data
    result = response.json()['chart']['result'][0]
    timestamps = result['timestamp']
    opens = result['indicators']['quote'][0]['open']
    highs = result['indicators']['quote'][0]['high']
    lows = result['indicators']['quote'][0]['low']
    closes = result['indicators']['quote'][0]['close']
    volumes = result['indicators']['quote'][0]['volume']
    currency = result['meta']['currency']
    splits_dict = result['events']['splits'] if 'events' in result and 'splits' in result['events'] else []

    # Check if currency is null
    if currency is None:
        # Format the URL with the symbol, exchange, and epoch time
        currency_url = f"https://finance.yahoo.com/quote/{code}/history"

        response = requests.get(currency_url, headers=headers)
        soup = BeautifulSoup(response.text, "html.parser")

        span = soup.select_one("#quote-header-info > div > div > div > span").text.strip()
        pattern = r'Currency in (\w+)'
        match = re.search(pattern, span)
        currency = match.group(1)

        print(currency)


    # Create a DataFrame
    df = pd.DataFrame({
        'Date': pd.to_datetime(timestamps, unit='s').strftime('%Y-%m-%d'),
        'Open': opens,
        'High': highs,
        'Low': lows,
        'Close': closes,
        'Volume': volumes
    })

    # Add currency column
    df["Currency"] = currency

    # Remove duplicates from the DataFrame
    df.drop_duplicates(inplace=True)

    # Sort final dataframe according to date
    df.sort_values("Date", inplace=True)
    df.reset_index(drop=True, inplace=True)

    # Apply unaccounted for splits
    if len(splits_dict) > 0:
        for date, dict in splits_dict.items():
            # Convert the date to datetime object
            date = pd.to_datetime(float(date), unit='s').strftime('%Y-%m-%d')

            # Calculate the multiplier
            multiplier = dict['numerator'] / dict['denominator']

            # Find the index of the entry that is greater than or equal to the target date
            index = df['Date'].searchsorted(date)

            # Retrieve the nearest entries to the target date
            previous_close = df.iloc[index - 1]['Close']
            next_close = df.iloc[index]['Close']

            # Finds if the difference is suspicious using a threshold
            threshold = 0.1
            percentage_diff = abs(previous_close / next_close)

            if percentage_diff <= 1 - threshold or percentage_diff >= 1 + threshold:
                # Filter the dataframe based on the date condition
                mask = df['Date'] < date
                df.loc[mask, ['Open', 'High', 'Low', 'Close']] *= multiplier

    # Round financial amounts to 2 decimal places
    df[['Open', 'High', 'Low', 'Close']] = df[['Open', 'High', 'Low', 'Close']].round(2)

    # Drop blank rows
    df.dropna(subset=['Open', 'High', 'Low', 'Close'], inplace=True)

    # Save dataframe to folder
    save_data(df, code, folder, True)


# Fetches all stocks information from eoddata.com

def eod_fetch_stock_data(eod_exchange, folder):
    pages_list = []

    headers = generate_header()

    # Initialize an empty DataFrame
    df = pd.DataFrame()

    url = f"https://eoddata.com/stocklist/{eod_exchange}.htm"

    # Send an HTTP GET request to the website
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        # Parse the HTML content using BeautifulSoup
        soup = BeautifulSoup(response.content, "html.parser")

        # Assuming 'soup' is the BeautifulSoup object containing the HTML content
        table = soup.select_one('table.lett').find("tr").find_all("td")
        pages_list = [lett.get_text() for lett in table]

    # Define solo page function
    def fetch_page_data(page):
        url = f"https://eoddata.com/stocklist/{eod_exchange}/{page}.htm"

        # Send an HTTP GET request to the website
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            # Parse the HTML content using BeautifulSoup
            soup = BeautifulSoup(response.content, "html.parser")

            tables = soup.find_all("table")

            if len(tables) >= 6:
                table = tables[5]
                data_rows = table.find_all("tr")

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

                # Add a "Page" column to mark the page of the data
                new_df["Page"] = page

                # Append new data to the existing DataFrame
                nonlocal df
                df = pd.concat([df, new_df])

    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = []
        for page in pages_list:
            futures.append(executor.submit(fetch_page_data, page))

        # Wait for all threads to finish
        concurrent.futures.wait(futures, return_when=concurrent.futures.ALL_COMPLETED)

        # Check if any pages are missing in the final DataFrame
        missing_pages = pages_list

        while missing_pages:
            with concurrent.futures.ThreadPoolExecutor() as executor:
                futures = []
                for page in missing_pages:
                    futures.append(executor.submit(fetch_page_data, page))

                # Wait for all threads to finish
                concurrent.futures.wait(futures, return_when=concurrent.futures.ALL_COMPLETED)

            # Check if any pages are missing in the final DataFrame
            included_pages = df["Page"].unique()
            missing_pages = [page for page in pages_list if page not in included_pages]

        df.drop(columns=["Page"], inplace=True)
        df.sort_values(by="Symbol", ascending=True, inplace=True)
        df.drop_duplicates(inplace=True)

        save_data(df, f"Symbols.{folder}", folder, True)


# Tests if the symbol has valid historical data on Yahoo Finance

def yh_process_symbol(code, end_date, duration):
    corrected_symbol_list = []
    rejected_symbol_list = []

    # Calculate the end date (yesterday)
    end_date = datetime.datetime.strptime(end_date, "%b %d, %Y")

    # Calculate the start date (one month before today)
    start_date = end_date - relativedelta(years=duration)

    # Convert dates to epoch format
    end_date_epoch = int(time.mktime(end_date.timetuple()))
    start_date_epoch = int(time.mktime(start_date.timetuple()))

    # Format the URL
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{code}?symbol={code}&period1={start_date_epoch}&period2={end_date_epoch}&useYfid=true&interval=1d&includePrePost=false&events=div%7Csplit%7Cearn&lang=en-US&region=US&crumb=eREX9CqAe3K&corsDomain=finance.yahoo.com"
    print(url)
    headers = generate_header()
    response = requests.get(url, headers=headers)

    try:
        timestamps = response.json()['chart']['result'][0]['timestamp']
        corrected_symbol_list.append(code)
        print(f"[{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Status: {code} Accepted")

    except Exception as e:
        rejected_symbol_list.append(code)
        print(f"[{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Status: {code} Rejected")

    return corrected_symbol_list, rejected_symbol_list

# Runs process_symbol concurrently to speed things up

def test_historical_data(symbol_list, end_date, duration, folder):
    corrected_symbol_list = []
    rejected_symbol_list = []

    with concurrent.futures.ThreadPoolExecutor() as executor:
        # Submit symbol processing tasks to the executor
        futures = [executor.submit(yh_process_symbol, symbol, end_date, duration) for symbol in symbol_list]

        # Wait for all threads to finish
        concurrent.futures.wait(futures, return_when=concurrent.futures.ALL_COMPLETED)

        # Retrieve results from completed tasks
        for future in concurrent.futures.as_completed(futures):
            result_corrected, result_rejected = future.result()
            corrected_symbol_list.extend(result_corrected)
            rejected_symbol_list.extend(result_rejected)

    # Remove duplicates from corrected_symbol_list and rejected_symbol_list
    corrected_symbol_list = list(set(corrected_symbol_list))
    rejected_symbol_list = list(set(rejected_symbol_list))
    print(f"[{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Final: {len(corrected_symbol_list)}/{len(corrected_symbol_list) + len(rejected_symbol_list)} Valid")

    save_data(pd.DataFrame(corrected_symbol_list, columns=None), f"Correct_Symbols.{folder}", folder, False)
    save_data(pd.DataFrame(rejected_symbol_list, columns=None), f"Rejected_Symbols.{folder}", folder, False)


# Goes through all saved historical data and removed stocks with empty data

def remove_blank_historical_data(folder):
    correct_file_path = f"{folder}/Correct_Symbols.{folder}.csv"
    rejected_file_path = f"{folder}/Rejected_Symbols.{folder}.csv"

    for file_name in os.listdir(folder):
        if file_name not in [f"Symbols.{folder}", f"Correct_Symbols.{folder}", f"Rejected_Symbols.{folder}"]:
            df = pd.read_csv(f"{folder}/{file_name}")
            if len(df) == 0:
                # Remove symbol from Correct_Symbols file
                df_corrected = pd.read_csv(correct_file_path)
                df_corrected = df_corrected[df_corrected != file_name.split(".")[0]]
                df_corrected.dropna(inplace=True)
                df_corrected.to_csv(correct_file_path, index=False)

                # Add symbol to Rejected_Symbols file
                df_rejected = pd.read_csv(rejected_file_path)
                df_rejected = pandas.concat([df_rejected, file_name.split(".")[0]])
                df_rejected.to_csv(rejected_file_path, index=False)

                # Delete the file
                file_to_delete = f"{folder}/{file_name}"
                if os.path.exists(file_to_delete):
                    os.remove(file_to_delete)

                print(f"[{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {file_name}: Removed")


# Figures out all the foreign currency listings present in the exchange and fetches historical data for them

def search_fx_data(end_date, duration, folder, base_currency):
    fx_list = []
    symbols_list = []

    # Loop through all files, make list
    for file_name in os.listdir(folder):
        if file_name not in [f"Symbols.{folder}", f"Correct_Symbols.{folder}", f"Rejected_Symbols.{folder}"]:
            symbols_list.append(file_name.split(".")[0])
    def process_symbol(symbol):
        df = pd.read_csv(f"{folder}/{symbol}.csv")
        currency = df.iloc[0, 6]
        if currency != base_currency:
            fx_list.append(currency)

    with concurrent.futures.ThreadPoolExecutor() as executor:
        # Submit symbol processing tasks to the executor
        futures = [executor.submit(process_symbol, symbol) for symbol in symbols_list]

        # Wait for all tasks to complete
        concurrent.futures.wait(futures)

    fx_list = list(set(fx_list))

    def fetch_fx_data(fx):
        code = base_currency + fx + "%3DX"
        yh_fetch_historical_data(code, end_date, duration, "FX")

    with concurrent.futures.ThreadPoolExecutor() as executor:
        # Submit fetch tasks to the executor
        futures = [executor.submit(fetch_fx_data, fx) for fx in fx_list]

        # Wait for all tasks to complete
        concurrent.futures.wait(futures)


# Processes each stock's Yahoo Finance historical data by:
# 1) Changing all the currencies to base currency
# 2) Adds Change, Previous Change, $ Volume and 10D $ Volume columns

def yh_process_historical_data(code, folder, base_currency):
    df = pd.read_csv(f"{folder}/{code}.csv")
    currency = df.iloc[0, 6]

    if currency != base_currency:
        fx_file_path = f"FX/{base_currency}{currency}%3DX.csv"
        fx_df = pd.read_csv(fx_file_path)

        for index in range(len(df)):
            row_date = df.loc[index, 'Date']

            fx_entry = fx_df.loc[fx_df['Date'] == row_date]

            if fx_entry.empty:
                smaller_dates = fx_df.loc[fx_df['Date'] < row_date, 'Date']
                if len(smaller_dates) > 0:
                    nearest_date = np.max(smaller_dates)
                    fx_entry = fx_df.loc[fx_df['Date'] == nearest_date]

            fx_rate = fx_entry['Close'].values[0]

            df.loc[index, 'Open'] = round(df.loc[index, 'Open'] / fx_rate, 2)
            df.loc[index, 'High'] = round(df.loc[index, 'High'] / fx_rate, 2)
            df.loc[index, 'Low'] = round(df.loc[index, 'Low'] / fx_rate, 2)
            df.loc[index, 'Close'] = round(df.loc[index, 'Close'] / fx_rate, 2)
            df.loc[index, 'Currency'] = base_currency

    # Drop the Currency column
    df = df.drop('Currency', axis=1)
    df['% Day Change'] = round(df['Close'] / df['Open'] - 1, 4)
    df['% Previous Change'] = round(df['Close'] / df['Close'].shift(1) - 1, 4)
    df['$ Volume'] = round(df['Close'] * df['Volume'], 2)
    df['10D $ Volume'] = round(df['$ Volume'].rolling(window=10).mean(), 2)
    df['Previous 10D $ Volume'] = df['10D $ Volume'].shift(1)
    df = df.drop('10D $ Volume', axis=1)
    df = df.iloc[:-1]

    save_data(df, f"{code}", folder, True)


# Multithread to fetch historical data from MarketWatch

def mw_process_chunk(chunk_start_date, chunk_end_date, symbol, country_code):
    # Format the start_date and end_date strings
    chunk_start_date_str = chunk_start_date.strftime("%#m/%#d/%Y")
    chunk_end_date_str = chunk_end_date.strftime("%#m/%#d/%Y")

    # Format the URL with the symbol and dates
    url = f"https://www.marketwatch.com/investing/index/{symbol}/downloaddatapartial?partial=true&index=0&countryCode={country_code}&iso=&startDate={chunk_start_date_str}&endDate={chunk_end_date_str}&frequency=null&downloadPartial=false&csvDownload=false&newDates=true"

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
    chunk_data = []
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
            chunk_data.append(row_dict)

    return chunk_data


# Fetches historical data from MarketWatch

def mw_fetch_historical_data(type, symbol, end_date, duration, country_code="", chunk_size=30):
    # Parse the end_date string to a datetime object
    end_date = datetime.datetime.strptime(end_date, "%b %d, %Y")

    # Calculate the start date based on the duration
    start_date = max(end_date - datetime.timedelta(days=duration * 365),
                     datetime.datetime.strptime("Dec 1, 2003", "%b %d, %Y"))

    # Initialize an empty list to store the historical data chunks
    chunks = []

    # Create date ranges for fetching data in smaller chunks
    while end_date >= start_date:
        # Calculate the start and end dates for the current chunk
        chunk_end_date = end_date
        chunk_start_date = end_date - datetime.timedelta(days=chunk_size - 1)
        if chunk_start_date < start_date:
            chunk_start_date = start_date

        chunks.append((chunk_start_date, chunk_end_date))
        end_date = chunk_start_date - datetime.timedelta(days=1)

    # Fetch data in smaller date ranges concurrently
    with concurrent.futures.ThreadPoolExecutor() as executor:
        # Submit tasks to the executor
        futures = []
        for chunk_start_date, chunk_end_date in chunks:
            futures.append(executor.submit(mw_process_chunk, chunk_start_date, chunk_end_date, symbol, country_code))

        # Get the results from the completed tasks
        chunk_data_list = [future.result() for future in concurrent.futures.as_completed(futures)]

    # Combine the chunk data into a single DataFrame
    df = pd.DataFrame()
    for chunk_data in chunk_data_list:
        df = df.append(chunk_data, ignore_index=True)

    # Change the order of columns
    df = df[['Date', 'Open', 'High', 'Low', 'Close']]

    # Convert numeric columns to appropriate data types
    df["Open"] = pd.to_numeric(df["Open"])
    df["High"] = pd.to_numeric(df["High"])
    df["Low"] = pd.to_numeric(df["Low"])
    df["Close"] = pd.to_numeric(df["Close"])

    # Format the 'Date' column without leading zeros
    df['Date'] = df['Date'].str.strip()
    df['Date'] = df['Date'].str.split('\n').str[0]
    df['Date'] = pd.to_datetime(df['Date'], format='%m/%d/%Y')

    # Add Previous Change
    df['% Previous Change'] = round(df['Close'] / df['Close'].shift(1) - 1, 4)

    df = df.sort_values('Date', ascending=True)

    save_data(df, symbol, type, True)


# Given an investment value and pricing_mode, it calculates the fees needed for SGX on IBKR

def ibkr_sg_fees(investment_value, pricing_mode, monthly_trade_value=0):
    gst = 0.09
    total_fees = 0

    if pricing_mode == "fixed":
        minimum = 2.5
        fees = 0.0008

        total_fees += max(investment_value * fees, minimum) * (1 + gst)
    elif pricing_mode == "tiered":
        exchange_transaction_fee = 0.00034775
        exchange_access_fee = 0.00008025
        total_fees = investment_value * (exchange_transaction_fee + exchange_access_fee)

        if monthly_trade_value <= 2500000:
            minimum = 2.5
            fees = 0.0008

            total_fees += max(investment_value * fees, minimum) * (1 + gst)
        elif 2500000 < monthly_trade_value <= 50000000:
            minimum = 1.6
            fees = 0.0005

            total_fees += max(investment_value * fees, minimum) * (1 + gst)
        elif 50000000 < monthly_trade_value <= 150000000:
            minimum = 1.2
            fees = 0.0003

            total_fees += max(investment_value * fees, minimum) * (1 + gst)
        elif 150000000 < monthly_trade_value:
            minimum = 0.9
            fees = 0.0002

            total_fees += max(investment_value * fees, minimum) * (1 + gst)

    return round(total_fees, 2)

# Given a share_price, quantity and pricing_mode, it calculates the fees needed for US stocks on IBKR

def ibkr_us_fees(share_price, quantity, pricing_mode, monthly_trade_volume=0):
    investment_value = share_price * quantity
    total_fees = 0

    # Calculate fixed pricing mode fees
    if pricing_mode == "fixed":
        # Regulatory Fees
        sec_transaction_fee = 0.000008 * investment_value
        finra_tradiing_activity_fee = 0.000145 * quantity
        regulatory_fees = sec_transaction_fee + finra_tradiing_activity_fee

        # IBKR Fees
        per_share = 0.005  # $
        minimum = 1  # $ per order
        maximum = 0.01  # % per order
        ibkr_fees = min(max(per_share * quantity, minimum), maximum * investment_value)

        # Calculate fees
        total_fees += ibkr_fees + regulatory_fees

    # Calculate tiered pricing mode fees
    elif pricing_mode == "tiered":
        # Regulatory Fees
        sec_transaction_fee = 0.000008 * investment_value
        finra_tradiing_activity_fee = 0.000145 * quantity
        regulatory_fees = sec_transaction_fee + finra_tradiing_activity_fee

        # Exchange Fees (assuming remove liquidity and normal execution not MOO or MOC)
        exchange_fees = 0
        if share_price < 1:
            exchange_fees += 0.003 * investment_value
        else:
            exchange_fees += 0.003 * quantity

        # Clearing fees
        nscc_dtc_fees = 0.0002 * quantity
        clearing_fees = nscc_dtc_fees

        # IBKR Fees
        ibkr_fees = 0
        minimum = 0.35  # $ per order
        maximum = 0.01  # % per order
        if monthly_trade_volume <= 300000:
            fees = 0.0035 # $ per share
            ibkr_fees += fees * quantity
        elif 300000 < monthly_trade_volume <= 3000000:
            fees = 0.002 # $ per share
            ibkr_fees += fees * quantity
        elif 3000000 < monthly_trade_volume <= 20000000:
            fees = 0.0015 # $ per share
            ibkr_fees += fees * quantity
        elif 20000000 < monthly_trade_volume <= 100000000:
            fees = 0.001 # $ per share
            ibkr_fees += fees * quantity
        elif 100000000 < monthly_trade_volume:
            fees = 0.0005  # $ per share
            ibkr_fees += fees * quantity
        ibkr_fees = min(max(ibkr_fees, minimum), maximum * investment_value)

        # Pass through fees
        nyse_pass_through_fees = 0.000175 * ibkr_fees
        finra_pass_through_fees = 0.00056 * ibkr_fees
        pass_through_fees = nyse_pass_through_fees + finra_pass_through_fees

        # Calculate Fees
        total_fees += regulatory_fees + exchange_fees + clearing_fees + ibkr_fees + pass_through_fees

    return round(total_fees, 2)

# Fetches table of security information from page

def mw_fetch_page(security_type, page_letter, page_number):
    # Generate headers
    headers = generate_header()

    # Format url
    url = f"https://www.marketwatch.com/tools/markets/{security_type}/a-z/{page_letter}/{page_number}"

    # Scrape the page
    response = requests.get(url, headers=headers)
    if response.status_code != 200:
        print(url)
    soup = BeautifulSoup(response.text, "html.parser")
    table = soup.find("table")
    data_rows = table.find("tbody").find_all("tr")

    # Organize scrape into list
    column_names = [th.get_text() for th in table.find_all("th")]
    data = []

    for row in data_rows:
        row_data = [td.get_text() for td in row.find_all("td")]
        data.append(row_data)

    # Organize list into dataframe
    page_df_columns = ['Name', 'Country', 'Exchange', 'Sector']
    page_df = pd.DataFrame(data, columns=page_df_columns)
    page_df['Symbol'] = page_df['Name'].str.extract(r'\(([\S]+)\)$')
    page_df['Name'] = page_df['Name'].str.extract(r'^([\s\S]+) \([\S]+\)$')
    page_df = page_df[['Symbol', 'Name', 'Country', 'Exchange', 'Sector']]

    print(f"[{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {security_type}: {page_letter} Page {page_number} Completed")
    return page_df

# Fetches the number of pages for a given A-Z page

def mw_fetch_process_pages(security_type, page_letter):
    # Generate headers
    headers = generate_header()

    # Format url
    url = f"https://www.marketwatch.com/tools/markets/{security_type}/a-z/{page_letter}"

    # Get the page numbers
    response = requests.get(url, headers=headers)
    soup = BeautifulSoup(response.text, "html.parser")
    li = None
    num_list = []
    if soup.select_one("#marketsindex > ul.pagination") is not None:
        li = soup.select_one("#marketsindex > ul.pagination").find_all("li")
        num_list = [num.get_text() for num in li]
    else:
        num_list.append(1)

    # Execute only if there are multiple pages
    if len(num_list) > 1:
        # Properly formatting page numbers and excluding noise
        kick_list = []
        add_list = []
        for index, num in enumerate(num_list):
            if num == "«" or num == "»":
                kick_list.append(num)
            elif num.find("-") != -1:
                start_page = int(num.split("-")[0])
                end_page = int(num.split("-")[1])
                page_numbers = list(range(start_page, end_page + 1))
                kick_list.append(num)
                [add_list.append(page) for page in page_numbers]
            else:
                num_list[index] = int(num)
        for num in kick_list:
            num_list.remove(num)
        for num in add_list:
            num_list.append(num)

    # Fetch data in each page
    with concurrent.futures.ThreadPoolExecutor() as executor:
        # Submit tasks to the executor
        futures = []
        for num in num_list:
            futures.append(executor.submit(mw_fetch_page, security_type, page_letter, num))

        # Wait for all tasks to complete
        concurrent.futures.wait(futures)

        # Get the results from the completed tasks
        security_data_list = [future.result() for future in futures]

    # Combine the chunk data into a single DataFrame
    az_page_df = pd.DataFrame()
    for data in security_data_list:
        az_page_df = pd.concat([data, az_page_df])

    # Remove duplicates from dataframe
    az_page_df = az_page_df.drop_duplicates()

    return az_page_df

# Fetches all available securities on MarketWatch given a security type

def mw_fetch_security_list(security_type):
    # Generate headers
    headers = generate_header()

    # Column headers for DataFrame
    df_columns = ['Symbol', 'Name', 'Country', 'Exchange', 'Sector']

    # Initialize an empty DataFrame
    df = pd.DataFrame(columns=df_columns)

    # Format base url
    base_url = f"https://www.marketwatch.com/tools/markets/{security_type}/a-z"

    # Get the A-Z page names
    base_response = requests.get(base_url, headers=headers)
    base_soup = BeautifulSoup(base_response.text, "html.parser")
    li = base_soup.select_one("#marketsindex > div > ul").find_all("li")
    az_list = [a.get_text() for a in li]
    az_list = [az.replace(" (current)", "") for az in az_list]

    # Fetch data for each letter page
    with concurrent.futures.ThreadPoolExecutor() as executor:
        # Submit tasks to the executor
        futures = []
        for az in az_list:
            futures.append(executor.submit(mw_fetch_process_pages, security_type, az))

        # Wait for all tasks to complete
        concurrent.futures.wait(futures)

        # Get the results from the completed tasks
        security_data_list = [future.result() for future in futures]

    # Combine the chunk data into a single DataFrame
    for data in security_data_list:
        df = pd.concat([data, df])

    # Remove duplicates from dataframe
    df = df.drop_duplicates()

    # Sort dataframe by ascending symbols
    df = df.sort_values(by='Symbol', ascending=True).reset_index(drop=True)

    # Save dataframe
    save_data(df, security_type, "Securities List")

# Formats stock list from MarketWatch Symbols to Yahoo Finance Symbols for US stocks

def mw_format_yh_us_stocks():
    # Initialize df by opening file from Securities List
    df = pd.read_csv(f"Securities List/stocks.csv")

    # Filter only entries from NYSE and NASDAQ
    df = df[(df['Exchange'] == 'XNAS') | (df['Exchange'] == 'XNYS')].reset_index(drop=True)

    # Reformat Symbol
    df['Symbol'] = df['Symbol'].astype(str).apply(lambda x: re.sub(r'\.', '-', x))
    df['Symbol'] = df['Symbol'].astype(str).apply(lambda x: re.sub(r'([A-Z]+\-)([A-Z])R([A-Z])?', r'\1\2\3', x))
    df['Symbol'] = df['Symbol'].astype(str).apply(lambda x: re.sub(r'\-UT', r'-UN', x))

    # Save updated dataframe
    save_data(df, "Symbols.US", "US")
