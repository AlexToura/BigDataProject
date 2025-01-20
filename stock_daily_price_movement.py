from pymongo import MongoClient
import pandas as pd
from datetime import datetime

# Function to connect to MongoDB
def connect_to_mongo():
    # Replace with your MongoDB connection string
    client = MongoClient("mongodb+srv://alexandrostoura:jf0iMWpYnpuZzFzS@cluster0.kl4wk.mongodb.net/")
    db = client['BigDataProject']  # Replace with your actual database name
    return db

# Function to calculate and update price movement (close - open) in MongoDB
def update_price_movement(stock_ticker, db):
    # Access the stock's data collection (e.g., 'TSLA_stock_data')
    collection_name = f"{stock_ticker}_stock_data"
    collection = db[collection_name]

    # Load data from MongoDB into a Pandas DataFrame
    cursor = collection.find({})
    df = pd.DataFrame(list(cursor))

    # Check if the data exists
    if df.empty:
        print(f"No stock data found for {stock_ticker}")
        return

    # Convert the 'timestamp' field to datetime and extract the date part
    df['Date'] = pd.to_datetime(df['timestamp']).dt.date

    # Calculate the daily price movement (close - open) as percentage change
    df['day_price_movement'] = (df['Close'] - df['Open']) / df['Open']  # Percentage change

    # Prepare data to update MongoDB documents
    for index, row in df.iterrows():
        # Update each document with the 'day_price_movement' field
        collection.update_one(
            {'_id': row['_id']},  # Match the document by its _id
            {'$set': {'day_price_movement': row['day_price_movement']}}  # Set the new field
        )
        print(f"Updated price movement for {stock_ticker} on {row['Date']}")

# Main function to process all collections ending with '_stock_data'
def process_all_stocks():
    db = connect_to_mongo()  # Connect to MongoDB

    # Get all collections that end with '_stock_data'
    collections = [coll for coll in db.list_collection_names() if coll.endswith('_stock_data')]

    # Process each stock's stock data collection to update price movement
    for collection in collections:
        # Extract the stock ticker from the collection name (e.g., 'TSLA_stock_data' -> 'TSLA')
        stock_ticker = collection.replace('_stock_data', '')
        update_price_movement(stock_ticker, db)

# Run the process to update price movements for all stocks
if __name__ == "__main__":
    process_all_stocks()
