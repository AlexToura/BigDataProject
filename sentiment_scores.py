from pymongo import MongoClient
from textblob import TextBlob


# Connect to MongoDB
def connect_to_mongo():
    # Replace with your MongoDB connection string
    client = MongoClient("mongodb+srv://alexandrostoura:jf0iMWpYnpuZzFzS@cluster0.kl4wk.mongodb.net/")
    db = client['BigDataProject']  # Replace with your database name
    return db


# Function to calculate sentiment score using TextBlob
def get_sentiment_score(content):
    # Create a TextBlob object and get the sentiment polarity score
    blob = TextBlob(content)
    # Polarity score is between -1 (negative) and 1 (positive)
    sentiment_score = blob.sentiment.polarity
    return sentiment_score


# Function to process all collections for sentiment analysis
def process_all_collections():
    db = connect_to_mongo()

    # Get all collection names that end with '_news' (for example, 'TSLA_news', 'AAPL_news', etc.)
    collections = [coll for coll in db.list_collection_names() if coll.endswith('_news')]

    # Loop through each collection (for each ticker)
    for collection_name in collections:
        collection = db[collection_name]

        # Fetch all articles in the collection
        articles = collection.find()

        # Loop through each article
        for article in articles:
            # Perform sentiment analysis only if the article has content
            if article.get('content'):
                sentiment_score = get_sentiment_score(article['content'])

                # Update the article with the sentiment score
                collection.update_one(
                    {"_id": article["_id"]},  # Filter by the article's unique ID
                    {"$set": {"sentiment_score": sentiment_score}}  # Set the sentiment_score field
                )
                print(f"Updated sentiment score for article: {article['title']}")
            else:
                print(f"No content for article: {article['title']}")

    print("Sentiment analysis completed for all articles.")


# Run the sentiment analysis on all articles in all ticker collections
if __name__ == "__main__":
    process_all_collections()
