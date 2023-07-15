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
from functions import *
from dateutil.relativedelta import relativedelta

base_currency = "SGD"
eod_exchange = "SGX"
yh_exchange = "SI"
end_date = "Jul 12, 2023"
duration = 20
file_path = f"{yh_exchange}/Symbols.{yh_exchange}.csv"
correct_file_path = f"{yh_exchange}/Correct_Symbols.{yh_exchange}.csv"
column_index = 0  # Index of the column to extract
execution_index = "sti"
reference_index = "spx"


