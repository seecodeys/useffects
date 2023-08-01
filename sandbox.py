import csv
import threading
import json

import numpy
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
from concurrent.futures import ThreadPoolExecutor
from dateutil.relativedelta import relativedelta
from functions import *


# Accepts securities dataframe and securities symbol types dataframe and adds corresponding column to the former

def symbol_regex_substitution(securities_list_file_path, symbol_types_file_path):
    # Initialize securities list dataframe
    securities_list_df = pd.read_csv(securities_list_file_path, keep_default_na=False, dtype=str)

    # Initialize symbol types dataframe
    symbol_types_df = pd.read_csv(symbol_types_file_path, keep_default_na=False, dtype=str)

    # Applying identify_regex_symbol to each entry in securities_list_df['Symbol'] and saving the result to 'Regex' column
    securities_list_df['Regex'] = securities_list_df['Symbol'].apply(lambda symbol: identify_regex_symbol(symbol))

    # Applying process_symbol_regex_substitution_finder to each entry in securities_list_df
    securities_list_df[['Yahoo Exchange', 'Regex Substitution']] = securities_list_df.apply(lambda entry: process_symbol_regex_substitution_finder(entry['Exchange'], entry['Regex'], symbol_types_df), axis=1, result_type='expand')

    # Filter out entries where 'Regex Substitution' is N/A
    securities_list_df = securities_list_df[securities_list_df['Regex Substitution'] != "N/A"]

    # Applying process_symbol_regex_substitution to each entry in securities_list_df
    securities_list_df['Yahoo Symbol'] = securities_list_df.apply(lambda entry: process_symbol_regex_substitution(entry['Symbol'], entry['Name'], entry['Regex Substitution']), axis=1)

    print(securities_list_df)
    save_data(securities_list_df, "stocks_test", "Securities List")
    print(symbol_types_df)

symbol_regex_substitution("Securities List/stocks.csv", "Securities List/stocks_symbol_types.csv")
# name = "RT Exports Ltd."
# urls = [
#         name.split('Ltd.')[0].strip().replace(' ', '%20'),
#         name.split('Ltd.')[0].strip().replace('&', '%26').replace(' ', '%20'),
#         name.replace('Ltd.', 'Limited').split('Ltd.')[0].strip().replace(' ', '%20'),
#         ' '.join(name.split(' ')[:3]).split('Ltd.')[0].strip().replace(' ', '%20'),
#         ''.join(f'{char}.' if char.isupper() and (index + 1 < len(name) and (name[index + 1].isupper() or name[index + 1] == ' ')) else char for index, char in enumerate(name.replace('Ltd.', 'Limited'))).replace('. ', '.').replace(' ', '%20'),
#         ''.join(f'{char}.' if char.isupper() and (i + 1 < len(name) and (name[i + 1].isupper() or name[i + 1] == ' ')) else char for i, char in enumerate(name.replace('Ltd.', 'Limited'))).replace('Ltd.', '').replace('. ', '.').replace(' ', '%20').replace('.', '. '),
#         re.sub(r"(?<=\w)([A-Z])", r" \1", name).split('Ltd.')[0].strip().replace(' ', '%20'),
#         name.split('Ltd.')[0].strip().replace(' ', '%20').replace('.', ''),
#         name.split('Ltd.')[0].strip()[:-1].replace(' ', '%20'),
#         name.split('Ltd.')[0].strip().replace('ies', '').replace(' ', '%20'),
#         name.split('Ltd.')[0].strip().replace('&', ' and ').replace(' ', '%20')
#     ]
#
# print(str(''.join(f'{char}.' if char.isupper() and (index + 1 < len(name) and (name[index + 1].isupper() or name[index + 1] == ' ')) else char for index, char in enumerate(name.replace('Ltd.', 'Limited'))).replace('. ', '.').rsplit('.', 1)[0] + '. ' + ''.join(f'{char}.' if char.isupper() and (index + 1 < len(name) and (name[index + 1].isupper() or name[index + 1] == ' ')) else char for index, char in enumerate(name.replace('Ltd.', 'Limited'))).replace('. ', '.').rsplit('.', 1)[-1] if '.' in ''.join(f'{char}.' if char.isupper() and (index + 1 < len(name) and (name[index + 1].isupper() or name[index + 1] == ' ')) else char for index, char in enumerate(name.replace('Ltd.', 'Limited'))).replace('. ', '.') else ''.join(f'{char}.' if char.isupper() and (index + 1 < len(name) and (name[index + 1].isupper() or name[index + 1] == ' ')) else char for index, char in enumerate(name.replace('Ltd.', 'Limited'))).replace('. ', '.')).replace(' ', '%20'))

# print(SequenceMatcher(None, "JTL Infra".split('Ltd.')[0].strip(), "JTL Industries").ratio())




