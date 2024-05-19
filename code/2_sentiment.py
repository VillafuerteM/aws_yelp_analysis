import pandas as pd
from textblob import TextBlob

# Load your CSV file
data = pd.read_csv('../data/processed/yelp_ihop.csv')

# Function to calculate sentiment
def sentiment_analysis(text):
    return TextBlob(text).sentiment.polarity

# Apply sentiment analysis to the 'text' column
data['sentiment'] = data['text'].apply(sentiment_analysis)

# Save the results back to a CSV file
data.to_csv('../data/processed/yelp_ihop_sentiment.csv', index=False)



