import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta, timezone

def fetch_and_store_stock_data(ticker, db):
    """
    Fetch daily stock data (open and close prices) for the last 30 days
    and store it in MongoDB collections for the given ticker.

    :param ticker: The ticker symbol (e.g., 'TSLA', 'AAPL')
    :param db: MongoDB database object to store the data
    """
    # Get today's date and calculate the date 30 days ago
    today = datetime.today()
    thirty_days_ago = today - timedelta(days=30)

    # Retrieve the stock data for the current ticker (daily data for the last 30 days)
    try:
        ticker_data = yf.Ticker(ticker).history(start=thirty_days_ago.strftime('%Y-%m-%d'),
                                                 end=today.strftime('%Y-%m-%d'),
                                                 interval="1d")

        # Add a column with the ticker symbol to the data
        ticker_data.insert(0, "Ticker", ticker)

        # Print out the first few rows of the data for debugging
        print(f"Fetched data for {ticker}:")
        print(ticker_data.head())

    except Exception as e:
        print(f"An error occurred while retrieving data for {ticker}: {e}")
        return

    # The 'Date' column is actually the index in the DataFrame, so reset the index to make it a column
    ticker_data.reset_index(inplace=True)

    # Insert the 'timestamp' column in the first position (with proper datetime format)
    ticker_data.insert(0, 'timestamp', None)

    # Iterate over the rows of the DataFrame and convert the timestamps
    for index, row in ticker_data.iterrows():
        # Access the date from the index (now it's a regular column after reset_index)
        timestamp = row['Date']
        dt = datetime.strptime(str(timestamp), '%Y-%m-%d %H:%M:%S%z')

        # Convert the timestamp to UTC
        dt_utc = dt.astimezone(timezone.utc)

        # Use strftime to format the datetime object
        formatted_string = dt_utc.strftime("%Y-%m-%dT%H:%M:%SZ")
        dt = datetime.strptime(formatted_string, '%Y-%m-%dT%H:%M:%SZ')

        # Store the formatted string in the new column
        ticker_data.at[index, 'timestamp'] = dt

    # Drop the 'Date' column since it's now handled
    ticker_data = ticker_data.drop(columns=['Date'])

    # Prepare data for MongoDB as a list of dictionaries
    data_dict = ticker_data.to_dict(orient="records")

    # Get the corresponding collection for the ticker (e.g., 'TSLA_stock_data')
    collection_name = f"{ticker}_stock_data"
    collection = db[collection_name]

    try:
        # Print out the collection name being used for debugging
        print(f"Inserting data into collection: {collection_name}")

        # Insert the data into the collection
        result = collection.insert_many(data_dict, ordered=True)

        # Print a message indicating the number of documents inserted and the current time
        print(f"Inserted {len(result.inserted_ids)} documents into the '{collection_name}' collection")
        print(f"Last modified: {datetime.utcnow()}")

    except Exception as e:
        print(f"An error occurred while inserting data for {ticker}: {e}")
