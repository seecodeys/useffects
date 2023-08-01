import concurrent.futures
import datetime
import random
import requests
import threading
import pandas as pd
from bs4 import BeautifulSoup


# Generates a random header

def generate_header():
    # print(f"[{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] generate_header(): Generating Header...")
    headers_list = [
        {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.45 Safari/537.36"
        },
        {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/95.0.4638.54 Safari/537.36"
        },
        {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/94.0.4606.81 Safari/537.36"
        },
        {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.45 Safari/537.36"
        },
        {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/95.0.4638.54 Safari/537.36"
        },
        {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/94.0.4606.81 Safari/537.36"
        },
        {
            "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.45 Safari/537.36"
        },
        {
            "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/95.0.4638.54 Safari/537.36"
        },
        {
            "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/94.0.4606.81 Safari/537.36"
        }
    ]

    headers = random.choice(headers_list)

    # print(f"[{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] generate_header(): Header Generated")
    return headers

# Generates the first working proxy available

def generate_free_proxy():
    print(f"[{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] generate_free_proxy(): Generating Valid Proxy...")
    url = "https://free-proxy-list.net/"

    # Create dataframe
    full_column_headers = ["IP Address", "Port", "Code", "Country", "Anonymity", "Google", "Https", "Last Checked"]
    column_headers = ["Address"]
    untested_df = pd.DataFrame(columns=column_headers)

    # Create a lock
    lock = threading.Lock()

    # Create a final variables
    final_address = None
    final_result = None

    # Fetches new proxies and adds them to untested_df
    def fetch_new_proxies():
        # print(f"[{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] generate_free_proxy(): Adding New Proxies...")

        nonlocal untested_df
        response = requests.get(url)
        soup = BeautifulSoup(response.content, "html.parser")
        table_rows = soup.select_one("#list > div > div.table-responsive > div > table").find_all("tr")

        # Extract the data rows
        data_rows = table_rows[1:]
        data = []
        for row in data_rows:
            cells = row.find_all("td")
            row_data = [cell.text for cell in cells]
            data.append(row_data)

        # Create dataframe for fetch data
        fetch_df = pd.DataFrame(data, columns=full_column_headers)

        # Filter dataframe
        fetch_df = fetch_df[(fetch_df['Anonymity'] != "transparent")]
        fetch_df['Address'] = fetch_df['IP Address'] + ":" + fetch_df['Port']
        fetch_df.reset_index(drop=True, inplace=True)
        fetch_df = pd.DataFrame({"Address": fetch_df['Address']})

        # Filter out new proxies
        fetch_df = fetch_df[~fetch_df.index.isin(untested_df.index)]

        # Acquire the lock before modifying the shared DataFrames
        with lock:
            # Append new proxies onto untested_df
            untested_df = pd.concat([fetch_df, untested_df], ignore_index=True, sort=True)

        # print(f"[{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] generate_free_proxy(): New Proxies Added")

    # Tests a given proxy
    def test_proxy(address):
        # print(f"[{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] generate_free_proxy(): Testing Proxy {address}...")

        nonlocal untested_df
        nonlocal final_result
        nonlocal final_address

        try:
            # Tries to access the URL to check if the response is valid
            test_url = "https://finance.yahoo.com/"
            request = requests.get(test_url, proxies={"http": address, "https": address}, timeout=(3, 3))

            if request.status_code == 200:
                # print(f"[{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] generate_free_proxy(): Proxy {address} Valid")

                # Set the flag to indicate a valid proxy is found
                final_result = {"http": address, "https": address}
                final_address = address

        except:
            pass

        # Acquire the lock before modifying the shared DataFrames
        with lock:
            # Removes address from untested_df
            untested_df = untested_df[untested_df['Address'] != address]

    # Tests new proxies in untested_df
    def test_new_proxies():
        # print(f"[{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] generate_free_proxy(): Testing New Proxies...")

        nonlocal untested_df
        nonlocal final_result

        # Create a ThreadPoolExecutor
        with concurrent.futures.ThreadPoolExecutor() as executor:
            # Submit the tasks to the executor and get the future objects
            futures = [executor.submit(test_proxy, address) for address in untested_df['Address'].tolist()]

            # Wait for the first valid proxy to be found
            for future in concurrent.futures.as_completed(futures):
                if final_result is not None:
                    break

            # Cancel the remaining pending tasks
            for future in futures:
                future.cancel()

            executor.shutdown(wait=False)

    while final_result == None:
        fetch_new_proxies()
        test_new_proxies()

    print(f"[{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] generate_free_proxy(): Proxy Generated - {final_address}")
    return final_result

# Generate proxy with paid SmartProxy

def generate_proxy():
    url = 'https://ipinfo.io/'
    username = 'sp0m1p5di6'
    password = 'q5mtDnj7A6jangWx4F'
    proxy = f"http://{username}:{password}@gate.visitxiangtan.com:7000"

    return {'http': proxy, 'https': proxy}

