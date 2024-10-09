import ssl
import certifi
import urllib.request
import pandas as pd
import yfinance as yf
import openpyxl
import math
import time
from requests.exceptions import RequestException
import os  # To handle file permissions

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
                return None, None
            price = data['Close'].iloc[-1]
            market_cap = stock.info.get('marketCap', None)
            return price, market_cap
        except (RequestException, KeyError) as e:
            print(f"Attempt {attempt + 1}: Issue fetching data for {ticker}: {e}. Retrying...")
            time.sleep(delay)
        except Exception as e:
            print(f"Error fetching data for {ticker}: {e}. Skipping.")
            return None, None
    return None, None

# Step 3: Build the final dataframe with real-time data
def build_final_dataframe(tickers, company_names):
    my_columns = ['Ticker', 'Company Name', 'Price', 'Market Capitalization', 'Number Of Shares to Buy']
    final_dataframe = pd.DataFrame(columns=my_columns)
    for index, (symbol, company_name) in enumerate(zip(tickers, company_names)):
        price, market_cap = get_stock_data(symbol)
        if price is not None and market_cap is not None:
            new_row = pd.DataFrame([[symbol, company_name, price, market_cap, 'N/A']], columns=my_columns)
            final_dataframe = pd.concat([final_dataframe, new_row], ignore_index=True)
        print(f"Processed {index + 1}/{len(tickers)}: {symbol}")
    return final_dataframe

# Step 4: Calculate the number of shares to buy for each stock
def calculate_shares_to_buy(final_dataframe, portfolio_size):
    position_size = portfolio_size / len(final_dataframe.index)
    for i in range(0, len(final_dataframe['Ticker'])):
        final_dataframe.loc[i, 'Number Of Shares to Buy'] = math.floor(position_size / final_dataframe['Price'][i])
    return final_dataframe

# Step 5: Export the final dataframe to Excel with openpyxl and save to system storage
def export_to_excel(final_dataframe):
    try:
        # Define the path to save the file to a specific system directory (you can modify this)
        file_path = os.path.join(os.path.expanduser("~"), 'Documents', 'recommended_trades.xlsx')  # Save in the Documents folder

        # Create the Excel writer object using openpyxl engine
        writer = pd.ExcelWriter(file_path, engine='openpyxl')

        # Save dataframe to Excel
        final_dataframe.to_excel(writer, sheet_name='Recommended Trades', index=False)
        writer.close()

        print(f"Excel file saved successfully as '{file_path}'")
        return file_path  # Return the path for later use
    except Exception as e:
        print(f"Error occurred while saving the Excel file: {e}")

# Change file permissions to make the file writable
def change_file_permissions(file_path):
    try:
        os.chmod(file_path, 0o644)  # Ensure read-write for owner, read-only for others
        print(f"Permissions changed for '{file_path}'")
    except Exception as e:
        print(f"Error changing file permissions: {e}")

# Step 6: Read the generated Excel file
def read_generated_excel(file_path):
    try:
        df = pd.read_excel(file_path)
        print(df)
        return df
    except Exception as e:
        print(f"Error occurred while reading the Excel file: {e}")

# Step 7: Run the trading strategy
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
        portfolio_size = float(input("Enter the value of your portfolio:"))

    final_dataframe = calculate_shares_to_buy(final_dataframe, portfolio_size)
    print(final_dataframe)

    file_path = export_to_excel(final_dataframe)

    # Change file permissions after saving
    if file_path:
        change_file_permissions(file_path)

    # Read the Excel file
    if file_path:
        read_generated_excel(file_path)

if __name__ == "__main__":
    run_trading_strategy()
