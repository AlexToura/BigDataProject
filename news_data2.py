import requests
from pymongo import MongoClient


# Function to fetch news for a given ticker for the last 30 days
def fetch_news_for_ticker(ticker, db):
    # Construct the NewsAPI URL for the ticker (last 30 days, sorted by publication date)
    api_key = 'dc1649058ad44178bfa6f62a54affb3b'  # Replace with your actual NewsAPI key
    url = f"https://newsapi.org/v2/everything?q={ticker}&from=2024-12-20&apiKey={api_key}"

    try:
        # Fetch the news articles using the URL
        response = requests.get(url)

        if response.status_code == 200:
            # Parse the JSON response
            data = response.json()

            # Check if the status is 'ok' and if articles are available
            if data['status'] == 'ok' and data['totalResults'] > 0:
                articles = data['articles']

                # Prepare a list of articles to insert into MongoDB
                news_list = []
                for article in articles:
                    news_item = {
                        "source": article.get("source", {}).get("name", ""),
                        "author": article.get("author", ""),
                        "title": article.get("title", ""),
                        "description": article.get("description", ""),
                        "url": article.get("url", ""),
                        "urlToImage": article.get("urlToImage", ""),
                        "publishedAt": article.get("publishedAt", ""),
                        "content": article.get("content", ""),
                        "ticker": ticker  # Store the ticker symbol
                    }
                    news_list.append(news_item)

                # Insert the news articles into MongoDB under the ticker-specific collection
                if news_list:
                    collection_name = f"{ticker}_news"  # Create a collection name based on the ticker
                    db[collection_name].insert_many(news_list)
                    print(f"Inserted {len(news_list)} articles into {collection_name}")
                else:
                    print(f"No news articles found for {ticker}")
            else:
                print(f"No articles found for {ticker} within the last 30 days.")
        else:
            print(f"Failed to fetch news for {ticker}. Status code: {response.status_code}")
    except Exception as e:
        print(f"Error fetching news for {ticker}: {e}")
