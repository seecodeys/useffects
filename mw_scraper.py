import concurrent.futures
import requests
from bs4 import BeautifulSoup
import pandas as pd
import datetime

# Fetches table of security information from page

def mw_fetch_page(security_type, page_letter, page_number):

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/94.0.4606.81 Safari/537.36"
    }

    url = f"https://www.marketwatch.com/tools/markets/{security_type}/a-z/{page_letter}/{page_number}"

    # Scrape the page
    response = requests.get(url, headers=headers)
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
    page_df['Symbol'] = page_df['Name'].str.extract(r'\(([\s\S]+)\)')
    page_df['Name'] = page_df['Name'].str.extract(r'([\s\S]+) \([\s\S]+\)')
    page_df = page_df[['Symbol', 'Name', 'Country', 'Exchange', 'Sector']]

    print(f"[{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {security_type}: {page_letter} Page {page_number} Completed")
    return page_df

mw_fetch_page("stocks", "A", 1)

# Fetches the number of pages for a given A-Z page

def mw_fetch_process_pages(security_type, page_letter):

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/94.0.4606.81 Safari/537.36"
    }

    url = f"https://www.marketwatch.com/tools/markets/{security_type}/a-z/{page_letter}"

    # Get the page numbers
    response = requests.get(url, headers=headers)
    soup = BeautifulSoup(response.text, "html.parser")
    li = soup.select_one("#marketsindex > ul.pagination").find_all("li")
    num_list = [num.get_text() for num in li]

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

    # Fetch data in smaller date ranges concurrently
    with concurrent.futures.ThreadPoolExecutor() as executor:
        # Submit tasks to the executor
        futures = []
        for num in num_list:
            futures.append(executor.submit(mw_fetch_page, security_type, page_letter, num))

        # Get the results from the completed tasks
        security_data_list = [future.result() for future in concurrent.futures.as_completed(futures)]

    # Combine the chunk data into a single DataFrame
    az_page_df = pd.DataFrame()
    for data in security_data_list:
        az_page_df = pd.concat([data, az_page_df])

    # Remove duplicates from dataframe
    az_page_df = az_page_df.drop_duplicates()

    return az_page_df

# Fetches all available stocks on MarketWatch

def mw_fetch_stock():

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/94.0.4606.81 Safari/537.36"
    }

    # Column headers for DataFrame
    df_columns = ['Symbol', 'Name', 'Country', 'Exchange', 'Sector']

    # Initialize an empty DataFrame
    df = pd.DataFrame(columns=df_columns)

    base_url = "https://www.marketwatch.com/tools/markets/stocks/a-z"

    # Get the A-Z page names
    base_response = requests.get(base_url, headers=headers)
    base_soup = BeautifulSoup(base_response.text, "html.parser")
    li = base_soup.select_one("#marketsindex > div > ul").find_all("li")
    az_list = [a.get_text() for a in li]
    az_list = [az.replace(" (current)", "") for az in az_list]

    print(az_list)




# mw_fetch_stock()



# # Fetches all available securities on MarketWatch given a category
#
# def mw_fetch_securities(security_type="all")