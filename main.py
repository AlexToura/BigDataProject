from pymongo import MongoClient
from stock_data import fetch_and_store_stock_data  # Import the function from the other file
#from news_data import fetch_news_for_ticker
from news_data2 import fetch_news_for_ticker

# MongoDB connection string
connection_string = "mongodb+srv://alexandrostoura:jf0iMWpYnpuZzFzS@cluster0.kl4wk.mongodb.net/"

# Connect to MongoDB
client = MongoClient(connection_string)

# Access your database
db = client['BigDataProject']  # Replace with your database name

# List of tickers (replace with your top 10 tickers)
tickers = ['TSLA', 'AAPL', 'AMZN', 'SPY', 'QQQ', 'MSFT', 'NVDA', 'GOOGL', 'SP500']

# Call the function to fetch and store stock data
#for ticker in tickers:
    #fetch_and_store_stock_data(ticker, db)


# Call the function to fetch and store news data for the tickers
for ticker in tickers:
    fetch_news_for_ticker(ticker, db)

# Optionally, verify collections were created
print(db.list_collection_names())

# Close the connection after the work is done
client.close()
