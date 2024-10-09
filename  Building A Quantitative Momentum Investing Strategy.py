import numpy as np
import pandas as pd
import yfinance as yf
import math
from scipy import stats
from statistics import mean
import ssl
import xlsxwriter

# SSL certificate workaround
ssl._create_default_https_context = ssl._create_unverified_context

# Load the S&P 500 tickers and company names directly from Wikipedia
url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
sp500 = pd.read_html(url)[0]
tickers = sp500['Symbol'].tolist()
company_names = sp500['Security'].tolist()

# Filter out tickers with special characters or suffixes like ".B"
tickers = [ticker.replace('.', '-') for ticker in tickers]


# Batch the ticker list into chunks of 100
def chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


# Create ticker groups for batch processing
symbol_groups = list(chunks(tickers, 100))
symbol_strings = [','.join(group) for group in symbol_groups]

# Dataframe for final output
hqm_columns = [
    'Ticker', 'Company Name', 'Price', 'One-Year Price Return',
    'One-Year Return Percentile', 'Six-Month Price Return', 'Six-Month Return Percentile',
    'Three-Month Price Return', 'Three-Month Return Percentile', 'One-Month Price Return',
    'One-Month Return Percentile', 'HQM Score', 'Number of Shares to Buy'
]
hqm_dataframe = pd.DataFrame(columns=hqm_columns)

# Batch process using yfinance to get the data
for i, symbol_string in enumerate(symbol_strings):
    tickers_data = yf.Tickers(symbol_string)
    for ticker in symbol_string.split(','):
        try:
            stock_data = tickers_data.tickers[ticker].history(period="max")  # Changed to 'max'
            if stock_data.empty:
                continue  # Skip if there's no data for the ticker

            # Calculate various returns using .iloc[] to access by position
            year_return = (stock_data['Close'].iloc[-1] - stock_data['Close'].iloc[0]) / stock_data['Close'].iloc[0]
            six_month_return = (stock_data['Close'].iloc[-1] - stock_data['Close'].iloc[-126]) / \
                               stock_data['Close'].iloc[-126] if len(stock_data) > 126 else 0
            three_month_return = (stock_data['Close'].iloc[-1] - stock_data['Close'].iloc[-63]) / \
                                 stock_data['Close'].iloc[-63] if len(stock_data) > 63 else 0
            one_month_return = (stock_data['Close'].iloc[-1] - stock_data['Close'].iloc[-21]) / \
                               stock_data['Close'].iloc[-21] if len(stock_data) > 21 else 0

            # Append stock data to the DataFrame using pd.concat instead of append()
            new_row = pd.Series([
                ticker,
                company_names[tickers.index(ticker)],  # Add company name
                stock_data['Close'].iloc[-1],
                year_return,
                'N/A',
                six_month_return,
                'N/A',
                three_month_return,
                'N/A',
                one_month_return,
                'N/A',
                'N/A',
                'N/A'  # Number of shares to buy (filled later)
            ], index=hqm_columns)

            hqm_dataframe = pd.concat([hqm_dataframe, pd.DataFrame([new_row])], ignore_index=True)

        except Exception as e:
            print(f"{ticker}: {str(e)}")  # Handle individual ticker errors gracefully

# Calculate the percentiles for each time period
time_periods = ['One-Year', 'Six-Month', 'Three-Month', 'One-Month']

for row in hqm_dataframe.index:
    for time_period in time_periods:
        hqm_dataframe.loc[row, f'{time_period} Return Percentile'] = stats.percentileofscore(
            hqm_dataframe[f'{time_period} Price Return'], hqm_dataframe.loc[row, f'{time_period} Price Return']) / 100

# Calculate the HQM Score (mean of the percentiles)
for row in hqm_dataframe.index:
    momentum_percentiles = [hqm_dataframe.loc[row, f'{time_period} Return Percentile'] for time_period in time_periods]
    hqm_dataframe.loc[row, 'HQM Score'] = mean(momentum_percentiles)

# Sort and select the top 50 momentum stocks
hqm_dataframe.sort_values('HQM Score', ascending=False, inplace=True)
top_50_df = hqm_dataframe[:50].reset_index(drop=True)


# Get portfolio size and calculate the number of shares to buy for each stock
def portfolio_input():
    global portfolio_size
    portfolio_size = input("Enter the value of your portfolio: ")
    try:
        val = float(portfolio_size)
    except ValueError:
        print("That's not a valid number! Please try again.")
        portfolio_input()


portfolio_input()

position_size = float(portfolio_size) / len(top_50_df.index)

for i in range(0, len(top_50_df['Ticker'])):
    top_50_df.loc[i, 'Number of Shares to Buy'] = math.floor(position_size / top_50_df['Price'][i])

# Writing the DataFrames to Excel using XlsxWriter

# Save the analysis of all S&P 500 companies
writer_all = pd.ExcelWriter('/Users/snehalpatil/Documents/all_sp500_analysis.xlsx', engine='xlsxwriter')
hqm_dataframe.to_excel(writer_all, sheet_name='All S&P 500 Analysis', index=False)

# Save the top 50 momentum stocks
writer_top_50 = pd.ExcelWriter('/Users/snehalpatil/Documents/top_50_momentum_stocks.xlsx', engine='xlsxwriter')
top_50_df.to_excel(writer_top_50, sheet_name='Top 50 Momentum Stocks', index=False)

# Formatting the Excel sheet
background_color = '#0a0a23'
font_color = '#ffffff'

string_template = writer_all.book.add_format({
    'font_color': font_color,
    'bg_color': background_color,
    'border': 1
})

dollar_template = writer_all.book.add_format({
    'num_format': '$0.00',
    'font_color': font_color,
    'bg_color': background_color,
    'border': 1
})

integer_template = writer_all.book.add_format({
    'num_format': '0',
    'font_color': font_color,
    'bg_color': background_color,
    'border': 1
})

percent_template = writer_all.book.add_format({
    'num_format': '0.0%',
    'font_color': font_color,
    'bg_color': background_color,
    'border': 1
})

# Apply formats to columns for both files
column_formats = {
    'A': ['Ticker', string_template],
    'B': ['Company Name', string_template],
    'C': ['Price', dollar_template],
    'D': ['One-Year Price Return', percent_template],
    'E': ['One-Year Return Percentile', percent_template],
    'F': ['Six-Month Price Return', percent_template],
    'G': ['Six-Month Return Percentile', percent_template],
    'H': ['Three-Month Price Return', percent_template],
    'I': ['Three-Month Return Percentile', percent_template],
    'J': ['One-Month Price Return', percent_template],
    'K': ['One-Month Return Percentile', percent_template],
    'L': ['HQM Score', integer_template],
    'M': ['Number of Shares to Buy', integer_template]
}

# Formatting for all S&P 500 companies analysis
for column in column_formats.keys():
    writer_all.sheets['All S&P 500 Analysis'].set_column(f'{column}:{column}', 20, column_formats[column][1])
    writer_all.sheets['All S&P 500 Analysis'].write(f'{column}1', column_formats[column][0], string_template)

# Formatting for top 50 momentum stocks
for column in column_formats.keys():
    writer_top_50.sheets['Top 50 Momentum Stocks'].set_column(f'{column}:{column}', 20, column_formats[column][1])
    writer_top_50.sheets['Top 50 Momentum Stocks'].write(f'{column}1', column_formats[column][0], string_template)

# Close both Excel writers (replacing save())
writer_all.close()
writer_top_50.close()

print("Analysis completed and saved as 'all_sp500_analysis.xlsx' and 'top_50_momentum_stocks.xlsx'.")
