from functions import *
from subfunctions import *

# Fully encompassing script that does the following:
# 1) Gets all the US symbols
# 2) For each US stock fetch + process them
# 3) Prepare orders for the day

def run_us_price_change_live_order(budget, folder, sensitivity, liquidity, stop_loss, max_fee, ibkr_pricing_mode, monthly_trade, reverse):
    # Fetch Securities List
    security_list_df = mw_fetch_security_list("stocks", False)
    security_list_df = mw_format_yh_us_stocks(security_list_df, False)

    # Initiate Symbols List
    symbols_list = security_list_df['Symbol'].values.tolist()

    # Filter incorrect symbols
    symbols_list = test_historical_data(symbols_list)

    print(security_list_df)

def main():
    budget = 100000
    folder = "live"
    sensitivity = 0.02
    liquidity = 0.000001
    stop_loss = 0.01
    max_fee = 0.002
    ibkr_pricing_mode = "tiered"
    monthly_trade = 0
    reverse = True

    time_function(run_us_price_change_live_order, budget, folder, sensitivity, liquidity, stop_loss, max_fee, ibkr_pricing_mode, monthly_trade, reverse)

if __name__ == "__main__":
    main()