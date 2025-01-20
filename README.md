# BigDataProject
In this project we created a cloud MongoDB database which was populated by following the steps as of below;
  1) Stock data for 8 famous tickers and for a 30-days period are downloaded from yahoo finance through python environment
  2) News for these tickers during given time period are downloaded through https://newsapi.org/ twice to take advantage of restricted requests :)
  3) We use Textblob library (NLP) to add a sentiment score to the contents of each of the news files
  4) We use Dask to group and aggregate (average) the sentiment score for each day for each ticker
  5) We calculate the percentage of the movement of the stock price for every day (Open-Close %)
  6) We calculate the correlation between price movement (positive or negative) to the average sentiment news score (also positive or negative) for each day based on date
  7) We also visualise the price movements against the sentiment scores

*Data was saved in MongoDB either in JSON or CSV format. Unfortunately fetching more news,for every day and with full context was not feasible due to strict pricing rules applied by the news APIs in general.In addition some news seemed irrelevant to the ticker, so a next step would be to clean them up if we can get also full access to article's contents.In that context,the results are not reflecting the reality 100% but at least an initial approach is attemoted,in trying to figure out the price movements related to the market news.
