import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

# Load data into DataFrame
data_series_1 = pd.read_csv('Portfolio Performance/reference.csv')

# Convert the 'Date' column to datetime format and set it as the index
data_series_1['Date'] = pd.to_datetime(data_series_1['Date'])
data_series_1.set_index('Date', inplace=True)

# Calculate daily returns
data_series_1['Daily Return'] = data_series_1['Balance'].pct_change()

# Remove the first row with NaN return as it doesn't have a previous day's balance for comparison
data_series_1.dropna(subset=['Daily Return'], inplace=True)

# Calculate the cumulative returns of the portfolio
data_series_1['Cumulative Returns'] = (1 + data_series_1['Daily Return']).cumprod()

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

# Plot the cumulative returns of the portfolio
plt.figure(figsize=(10, 6))
plt.plot(data_series_1.index, data_series_1['Cumulative Returns'], label='Portfolio 1')
plt.xlabel('Date')
plt.ylabel('Cumulative Returns')
plt.title('Cumulative Returns - Portfolio Performance')
plt.legend()
plt.grid(True)
plt.show()

# Print performance metrics
print("Portfolio 1 Performance Metrics:")
print("Total Return:", total_return_series_1)
print("Annualized Return:", annualized_return_series_1)
print("Annualized Volatility:", annualized_volatility_series_1)
print("Sharpe Ratio:", sharpe_ratio_series_1)
print("Maximum Drawdown:", max_drawdown_series_1)
