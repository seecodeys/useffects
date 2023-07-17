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


def yh_process_symbol_modified(code):
    corrected_symbol_list = []
    rejected_symbol_list = []

    # Get today's date
    today = datetime.date.today()

    # Calculate the end date (yesterday)
    end_date = today - datetime.timedelta(days=1)

    # Calculate the start date (one month before today)
    start_date = end_date - relativedelta(months=1)

    # Convert dates to epoch format
    end_date_epoch = int(time.mktime(end_date.timetuple()))
    start_date_epoch = int(time.mktime(start_date.timetuple()))

    # Format the URL
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{code}?symbol={code}&period1={start_date_epoch}&period2={end_date_epoch}&useYfid=true&interval=1d&includePrePost=false&events=div%7Csplit%7Cearn&lang=en-US&region=US&crumb=eREX9CqAe3K&corsDomain=finance.yahoo.com"
    headers = generate_header()
    response = requests.get(url, headers=headers)

    try:
        timestamps = response.json()['chart']['result'][0]['timestamp']
        corrected_symbol_list.append(code)
        # print(f"[{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Status: {code} Accepted")

    except Exception as e:
        print(response.status_code, code)

    return corrected_symbol_list, rejected_symbol_list