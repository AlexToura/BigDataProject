import pandas as pd
import matplotlib.pyplot as plt
from pymongo import MongoClient

# Function to connect to MongoDB
def connect_to_mongo():
    # Replace with your MongoDB connection string
    client = MongoClient("mongodb+srv://alexandrostoura:jf0iMWpYnpuZzFzS@cluster0.kl4wk.mongodb.net/")
    db = client['BigDataProject']  # Replace with your actual database name
    return db

# Load stock data and sentiment data for a given stock ticker
def load_data(stock_ticker, db):
    # Load stock data from MongoDB
    stock_collection_name = f"{stock_ticker}_stock_data"
    stock_collection = db[stock_collection_name]
    stock_cursor = stock_collection.find({})
    stock_df = pd.DataFrame(list(stock_cursor))

    # Load sentiment data from MongoDB
    sentiment_collection_name = f"{stock_ticker}_sentiment_avg"
    sentiment_collection = db[sentiment_collection_name]
    sentiment_cursor = sentiment_collection.find({})
    sentiment_df = pd.DataFrame(list(sentiment_cursor))

    # Convert 'timestamp' to date for stock data and 'publishedAt' for sentiment data
    stock_df['Date'] = pd.to_datetime(stock_df['timestamp']).dt.date
    sentiment_df['Date'] = pd.to_datetime(sentiment_df['publishedAt']).dt.date

    return stock_df, sentiment_df

# Merge stock data and sentiment data
def merge_data(stock_df, sentiment_df):
    # Merge on 'Date'
    merged_df = pd.merge(stock_df[['Date', 'day_price_movement']], sentiment_df[['Date', 'avg_sentiment_score']],
                         on='Date', how='inner')
    return merged_df

# Plot the time series of price movement vs sentiment for each ticker
def plot_time_series(merged_df, stock_ticker):
    plt.figure(figsize=(14, 7))

    # Plot price movement and sentiment score
    plt.plot(merged_df['Date'], merged_df['day_price_movement'], label='Price Movement', color='blue', linestyle='-', marker='o')
    plt.plot(merged_df['Date'], merged_df['avg_sentiment_score'], label='Sentiment Score', color='red', linestyle='--', marker='x')

    plt.title(f"Price Movement vs Sentiment Over Time for {stock_ticker}")
    plt.xlabel("Date")
    plt.ylabel("Price Movement (%)")
    plt.legend()
    plt.xticks(rotation=45)
    plt.grid(True)
    plt.show()

# Function to generate visualizations for all tickers
def visualize_for_all_tickers(tickers):
    db = connect_to_mongo()  # Connect to MongoDB

    # Iterate over all tickers and generate time series plot
    for stock_ticker in tickers:
        print(f"Generating time series plot for {stock_ticker}...")
        stock_df, sentiment_df = load_data(stock_ticker, db)
        merged_df = merge_data(stock_df, sentiment_df)
        plot_time_series(merged_df, stock_ticker)

# List of tickers to process
tickers = ['TSLA', 'AAPL', 'AMZN', 'SPY', 'QQQ', 'MSFT', 'NVDA', 'GOOGL']

# Run the process to generate visualizations for all tickers
visualize_for_all_tickers(tickers)
