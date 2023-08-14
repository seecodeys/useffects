import pandas as pd
import os
from subfunctions import *

for file_name in os.listdir("US"):
    modified_name = file_name.replace(f".csv", "")
    if modified_name not in [f"Symbols.US", f"Correct_Symbols.US", f"Rejected_Symbols.US"]:
        try:
            df = pd.read_csv("C:\\Users\\User\\Coding\\Trading\\useffects\\US\\" + file_name)
            df.insert(8, '% Market Open Change', round(df['Open'] / df['Close'].shift(1) - 1, 4))
            save_data(df, modified_name, "US")
        except:
            print(file_name)