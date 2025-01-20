import pandas as pd
from pymongo import MongoClient


# Function to connect to MongoDB
def connect_to_mongo():
    client = MongoClient("mongodb+srv://alexandrostoura:jf0iMWpYnpuZzFzS@cluster0.kl4wk.mongodb.net/")
    db = client['BigDataProject']  # Replace with your actual database name
    return db


# Load stock data and sentiment data for a given stock ticker
def load_data(stock_ticker, db):
    # Load stock data
    stock_collection_name = f"{stock_ticker}_stock_data"
    stock_collection = db[stock_collection_name]
    stock_cursor = stock_collection.find({})
    stock_df = pd.DataFrame(list(stock_cursor))

    # Load sentiment data
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


# Binarize the movements and sentiment scores (positive/negative)
def binarize_data(merged_df):
    # Create binary columns for price movement and sentiment score
    merged_df['positive_movement'] = (merged_df['day_price_movement'] > 0).astype(
        int)  # 1 if positive movement, 0 if not
    merged_df['negative_movement'] = (merged_df['day_price_movement'] < 0).astype(
        int)  # 1 if negative movement, 0 if not
    merged_df['positive_sentiment'] = (merged_df['avg_sentiment_score'] > 0).astype(
        int)  # 1 if positive sentiment, 0 if not
    merged_df['negative_sentiment'] = (merged_df['avg_sentiment_score'] < 0).astype(
        int)  # 1 if negative sentiment, 0 if not
    return merged_df


# Compute the correlation between positive/negative price movement and sentiment
def compute_correlation(merged_df):
    # Correlation between positive movement and positive sentiment
    positive_corr = merged_df['positive_movement'].corr(merged_df['positive_sentiment'])

    # Correlation between negative movement and negative sentiment
    negative_corr = merged_df['negative_movement'].corr(merged_df['negative_sentiment'])

    print(f"Correlation between positive price movement and positive sentiment: {positive_corr}")
    print(f"Correlation between negative price movement and negative sentiment: {negative_corr}")

    return positive_corr, negative_corr


# Main function to process the stock and sentiment data
def process_correlation(stock_ticker, db):
    # Load stock and sentiment data
    stock_df, sentiment_df = load_data(stock_ticker, db)

    # Merge data on 'Date'
    merged_df = merge_data(stock_df, sentiment_df)

    # Binarize the data
    merged_df = binarize_data(merged_df)

    # Compute correlation
    positive_corr, negative_corr = compute_correlation(merged_df)
    return positive_corr, negative_corr


# Function to store correlation results in MongoDB
def store_correlation_results(correlation_results, db):
    # Access the collection for storing results
    correlation_collection = db['correlation_results']

    # Insert the results into MongoDB
    correlation_collection.insert_many(correlation_results)
    print("Correlation results stored in MongoDB.")


# Process all tickers and calculate correlation for each
def process_all_stocks(tickers):
    db = connect_to_mongo()  # Connect to MongoDB

    # List to store correlation results for each ticker
    correlation_results = []

    # Iterate over all tickers and calculate correlation
    for stock_ticker in tickers:
        print(f"Processing {stock_ticker}...")
        positive_corr, negative_corr = process_correlation(stock_ticker, db)

        # Store the results in the list
        correlation_results.append({
            'ticker': stock_ticker,
            'positive_corr': positive_corr,
            'negative_corr': negative_corr
        })

    # Store the results in MongoDB
    store_correlation_results(correlation_results, db)


# List of tickers to process
tickers = ['TSLA', 'AAPL', 'AMZN', 'SPY', 'QQQ', 'MSFT', 'NVDA', 'GOOGL']

# Run the process to calculate correlation for all tickers
if __name__ == "__main__":
    process_all_stocks(tickers)
