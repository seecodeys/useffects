import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

# Get user input for file name
file_name = input("Enter File Name: ")

# Load data into DataFrame
data_series_1 = pd.read_csv(f'Portfolio Performance/{file_name}.csv')

# Convert the 'Date' column to datetime format and set it as the index
data_series_1['Date'] = pd.to_datetime(data_series_1['Date'], format='%d-%b-%Y')
data_series_1.set_index('Date', inplace=True)
data_series_1['Balance'] = data_series_1['Balance'].astype(str).str.replace(',', '').astype(float)

# Calculate daily returns
data_series_1['Daily Return'] = data_series_1['Balance'].pct_change()

# Remove the first row with NaN return as it doesn't have a previous day's balance for comparison
data_series_1.dropna(subset=['Daily Return'], inplace=True)

# Calculate the cumulative returns of the portfolio
data_series_1['Cumulative Returns'] = (1 + data_series_1['Daily Return']).cumprod()

# Calculate the breakdown of returns every year
data_series_1['Year'] = data_series_1.index.year
data_series_1['Yearly Cumulative Returns'] = data_series_1.groupby('Year')['Cumulative Returns'].transform(lambda x: x / x.iloc[0])

# Calculate the breakdown of returns every month
data_series_1['YearMonth'] = data_series_1.index.to_period('M')
data_series_1['Monthly Cumulative Returns'] = data_series_1.groupby('YearMonth')['Cumulative Returns'].transform(lambda x: x / x.iloc[0])

# Calculate the total return of the portfolio over the period
total_return_series_1 = data_series_1['Cumulative Returns'][-1] - 1

# Calculate the annualized return of the portfolio
num_days = (data_series_1.index[-1] - data_series_1.index[0]).days
annualized_return_series_1 = (1 + total_return_series_1) ** (365 / num_days) - 1

# Calculate the annualized volatility (standard deviation) of the portfolio
annualized_volatility_series_1 = data_series_1['Daily Return'].std() * np.sqrt(252)

# Calculate the Sharpe ratio of the portfolio (assuming a risk-free rate of 2%)
risk_free_rate = 0.02
sharpe_ratio_series_1 = (annualized_return_series_1 - risk_free_rate) / annualized_volatility_series_1


# Calculate the maximum drawdown of the portfolio
def calculate_max_drawdown(cumulative_returns):
    # Calculate the cumulative wealth index
    wealth_index = (1 + cumulative_returns).cumprod()
    # Calculate the previous peaks
    previous_peaks = wealth_index.cummax()
    # Calculate the drawdowns
    drawdowns = (wealth_index - previous_peaks) / previous_peaks
    # Calculate the maximum drawdown
    max_drawdown = drawdowns.min()
    return max_drawdown


max_drawdown_series_1 = calculate_max_drawdown(data_series_1['Daily Return'])

# Calculate the Sortino ratio
target_rate = 0.02  # Replace with your desired target rate (e.g., risk-free rate)
downside_returns = data_series_1['Daily Return'][data_series_1['Daily Return'] < target_rate]
downside_deviation = downside_returns.std() * np.sqrt(252)
sortino_ratio_series_1 = (annualized_return_series_1 - target_rate) / downside_deviation

# Plot the cumulative returns of the portfolio
plt.figure(figsize=(10, 6))
plt.plot(data_series_1.index, data_series_1['Cumulative Returns'], label='Portfolio 1')
plt.xlabel('Date')
plt.ylabel('Cumulative Returns')
plt.title('Cumulative Returns - Portfolio Performance')
plt.legend()
plt.grid(True)
plt.show()

# Plot the breakdown of returns every year
plt.figure(figsize=(10, 6))
for year, year_data in data_series_1.groupby('Year'):
    plt.plot(year_data.index, year_data['Yearly Cumulative Returns'], label=f'Year {year}')
plt.xlabel('Date')
plt.ylabel('Yearly Cumulative Returns')
plt.title('Yearly Cumulative Returns - Portfolio Performance')
plt.legend()
plt.grid(True)
plt.show()

# Plot the breakdown of returns every month
plt.figure(figsize=(10, 6))
for year_month, month_data in data_series_1.groupby('YearMonth'):
    plt.plot(month_data.index, month_data['Monthly Cumulative Returns'], label=f'Month {year_month}')
plt.xlabel('Date')
plt.ylabel('Monthly Cumulative Returns')
plt.title('Monthly Cumulative Returns - Portfolio Performance')
plt.legend()
plt.grid(True)
plt.show()

# Print performance metrics
print("Portfolio 1 Performance Metrics:")
print("Total Return:", total_return_series_1)
print("Annualized Return:", annualized_return_series_1)
print("Annualized Volatility:", annualized_volatility_series_1)
print("Sharpe Ratio:", sharpe_ratio_series_1)
print("Sortino Ratio:", sortino_ratio_series_1)
print("Maximum Drawdown:", max_drawdown_series_1)

# Print breakdown of returns every year
for year, year_data in data_series_1.groupby('Year'):
    year_return = year_data['Cumulative Returns'].iloc[-1] / year_data['Cumulative Returns'].iloc[0] - 1
    print(f"Year {year}: {year_return:.2%}")

# Print breakdown of returns every month
for year_month, month_data in data_series_1.groupby('YearMonth'):
    month_return = month_data['Cumulative Returns'].iloc[-1] / month_data['Cumulative Returns'].iloc[0] - 1
    print(f"Month {year_month}: {month_return:.2%}")
