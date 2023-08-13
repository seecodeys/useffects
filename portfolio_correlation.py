import pandas as pd

# Get user input for file name
file_name = input("Enter File Name: ")

# Load data into DataFrame
df = pd.read_csv(f'Portfolio Performance/{file_name}.csv')

# Convert percentage strings to floats
df['Custom'] = df['Custom'].str.rstrip('%').astype(float)
df['QQQ'] = df['QQQ'].str.rstrip('%').astype(float)
df['SPY'] = df['SPY'].str.rstrip('%').astype(float)

# Calculate the correlation between Custom and SPY
correlation_matrix_spy = df[['Custom', 'SPY']].corr()

# Calculate the correlation between Custom and QQQ
correlation_matrix_qqq = df[['Custom', 'QQQ']].corr()

print("Correlation Matrix:")
print(correlation_matrix_spy)
print(correlation_matrix_qqq)
