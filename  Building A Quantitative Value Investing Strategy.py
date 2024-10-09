import ssl
import certifi
import urllib.request
import pandas as pd
import yfinance as yf
import openpyxl
import math
import numpy as np
import os  # To handle file permissions
import time
from requests.exceptions import RequestException


# Step 1: Get the list of S&P 500 tickers and full company names using certifi for SSL verification
def get_sp500_tickers_and_names():
    url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
    ssl_context = ssl.create_default_context(cafile=certifi.where())

    with urllib.request.urlopen(url, context=ssl_context) as response:
        tables = pd.read_html(response)
        sp500_table = tables[0]
        tickers = sp500_table['Symbol'].tolist()
        company_names = sp500_table['Security'].tolist()

    tickers = [ticker.replace('.', '-') for ticker in tickers]
    return tickers, company_names


# Step 2: Get stock data using yfinance with retry logic
def get_stock_data(ticker, retries=3, delay=5):
    for attempt in range(retries):
        try:
            stock = yf.Ticker(ticker)
            data = stock.history(period='1d', timeout=10)
            if data.empty:
                print(f"No data for {ticker}. Skipping.")
                return None, None, None, None
            price = data['Close'].iloc[-1]
            pe_ratio = stock.info.get('trailingPE', None)
            pb_ratio = stock.info.get('priceToBook', None)
            ev_to_ebitda = stock.info.get('enterpriseToEbitda', None)
            ev_to_gp = stock.info.get('enterpriseToRevenue', None)
            return price, pe_ratio, pb_ratio, ev_to_ebitda, ev_to_gp
        except (RequestException, KeyError) as e:
            print(f"Attempt {attempt + 1}: Issue fetching data for {ticker}: {e}. Retrying...")
            time.sleep(delay)
        except Exception as e:
            print(f"Error fetching data for {ticker}: {e}. Skipping.")
            return None, None, None, None
    return None, None, None, None


# Step 3: Build the final dataframe with real-time data
def build_final_dataframe(tickers, company_names):
    my_columns = ['Ticker', 'Company Name', 'Price', 'Price-to-Earnings Ratio', 'Price-to-Book Ratio',
                  'EV/EBITDA', 'EV/GP', 'Number Of Shares to Buy']
    final_dataframe = pd.DataFrame(columns=my_columns)
    for index, (symbol, company_name) in enumerate(zip(tickers, company_names)):
        price, pe_ratio, pb_ratio, ev_to_ebitda, ev_to_gp = get_stock_data(symbol)
        if price is not None:
            new_row = pd.DataFrame([[symbol, company_name, price, pe_ratio, pb_ratio, ev_to_ebitda, ev_to_gp, 'N/A']],
                                   columns=my_columns)
            final_dataframe = pd.concat([final_dataframe, new_row], ignore_index=True)
        print(f"Processed {index + 1}/{len(tickers)}: {symbol}")
    return final_dataframe


# Step 4: Calculate percentiles
def calculate_percentiles(dataframe, column_name):
    dataframe[column_name] = pd.to_numeric(dataframe[column_name], errors='coerce')
    dataframe[column_name].fillna(np.inf, inplace=True)  # Replace NaNs with a large number to rank them lower
    dataframe[f'{column_name} Percentile'] = dataframe[column_name].rank(pct=True)
    return dataframe


# Step 5: Add valuation percentiles and RV score
def add_valuation_percentiles(final_dataframe):
    final_dataframe = calculate_percentiles(final_dataframe, 'Price-to-Earnings Ratio')
    final_dataframe = calculate_percentiles(final_dataframe, 'Price-to-Book Ratio')
    final_dataframe = calculate_percentiles(final_dataframe, 'EV/EBITDA')
    final_dataframe = calculate_percentiles(final_dataframe, 'EV/GP')
    return final_dataframe


# Step 6: Calculate RV Score
def calculate_rv_score(final_dataframe):
    final_dataframe['RV Score'] = final_dataframe[[
        'Price-to-Earnings Ratio Percentile',
        'Price-to-Book Ratio Percentile',
        'EV/EBITDA Percentile',
        'EV/GP Percentile'
    ]].mean(axis=1)
    return final_dataframe


# Step 7: Calculate the number of shares to buy
def calculate_shares_to_buy(final_dataframe, portfolio_size):
    position_size = portfolio_size / len(final_dataframe.index)
    for i in range(0, len(final_dataframe['Ticker'])):
        final_dataframe.loc[i, 'Number Of Shares to Buy'] = math.floor(position_size / final_dataframe['Price'][i])
    return final_dataframe


# Step 8: Export the final dataframe to Excel
def export_to_excel(final_dataframe, file_name):
    try:
        file_path = os.path.join(os.path.expanduser("~"), 'Documents', file_name)
        writer = pd.ExcelWriter(file_path, engine='openpyxl')
        final_dataframe.to_excel(writer, sheet_name='Recommended Trades', index=False)
        writer.close()
        print(f"Excel file saved successfully as '{file_path}'")
        return file_path
    except Exception as e:
        print(f"Error occurred while saving the Excel file: {e}")


# Step 9: Run the full strategy and save results
def run_trading_strategy():
    tickers, company_names = get_sp500_tickers_and_names()
    final_dataframe = build_final_dataframe(tickers, company_names)

    if final_dataframe.empty:
        print("No valid data fetched.")
        return

    portfolio_size = input("Enter the value of your portfolio: ")
    try:
        portfolio_size = float(portfolio_size)
    except ValueError:
        print("Invalid input. Please enter a number.")
        return

    final_dataframe = add_valuation_percentiles(final_dataframe)
    final_dataframe = calculate_rv_score(final_dataframe)
    final_dataframe = calculate_shares_to_buy(final_dataframe, portfolio_size)
    print(final_dataframe)

    export_to_excel(final_dataframe, 'all_sp500_companies.xlsx')

    # Save the top 50 value investing companies
    top_50_dataframe = final_dataframe.nsmallest(50, 'RV Score')
    export_to_excel(top_50_dataframe, 'top_50_value_investing_companies.xlsx')


if __name__ == "__main__":
    run_trading_strategy()
