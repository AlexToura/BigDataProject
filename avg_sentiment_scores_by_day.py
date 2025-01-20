import dask.dataframe as dd
import pandas as pd
from pymongo import MongoClient
import os

# Function to connect to MongoDB
def connect_to_mongo():
    # Replace with your MongoDB connection string
    client = MongoClient("mongodb+srv://alexandrostoura:jf0iMWpYnpuZzFzS@cluster0.kl4wk.mongodb.net/")
    db = client['BigDataProject']  # Replace with your actual database name
    return db

# Function to process each stock's news and calculate average sentiment score by day
def process_stock_news(stock_ticker, db):
    # Access the stock's news collection (e.g., 'TSLA_news')
    collection_name = f"{stock_ticker}_news"
    collection = db[collection_name]

    # Load data from MongoDB into a Dask DataFrame
    cursor = collection.find({})
    df = pd.DataFrame(list(cursor))
    dask_df = dd.from_pandas(df, npartitions=4)  # Specify the number of partitions for parallel processing

    # Check if data exists by computing the row count and checking if it's greater than 0
    row_count = dask_df.shape[0].compute()  # Compute the number of rows in the dataframe
    if row_count == 0:
        print(f"No data found for {stock_ticker}")
        return

    # Convert 'publishedAt' to date format (ignore time part) for grouping
    dask_df['publishedAt'] = dd.to_datetime(dask_df['publishedAt']).dt.date

    # Group by the 'publishedAt' date and calculate the average sentiment score
    sentiment_avg_df = dask_df.groupby('publishedAt')['sentiment_score'].mean().compute()

    # Show the results for debugging
    print(f"Sentiment score averages for {stock_ticker}:")
    print(sentiment_avg_df)

    # Prepare the data to insert into MongoDB
    sentiment_data = []
    for date, score in sentiment_avg_df.items():
        sentiment_data.append({
            'publishedAt': str(date),
            'avg_sentiment_score': score,
            'ticker': stock_ticker  # Store the ticker symbol for identification
        })

    # Save the results to MongoDB in a new collection for this stock
    result_collection_name = f"{stock_ticker}_sentiment_avg"
    result_collection = db[result_collection_name]

    # Insert the aggregated sentiment data into MongoDB
    if sentiment_data:
        result_collection.insert_many(sentiment_data)
        print(f"Saved average sentiment scores for {stock_ticker} into {result_collection_name}")

# Main function to process all collections ending with '_news'
def process_all_stocks():
    db = connect_to_mongo()  # Connect to MongoDB

    # Get all collections that end with '_news'
    collections = [coll for coll in db.list_collection_names() if coll.endswith('_news')]

    # Process each stock's news collection
    for collection in collections:
        # Extract the stock ticker from the collection name (e.g., 'TSLA_news' -> 'TSLA')
        stock_ticker = collection.replace('_news', '')
        process_stock_news(stock_ticker, db)

# Run the process to aggregate sentiment scores for all stocks
if __name__ == "__main__":
    process_all_stocks()
