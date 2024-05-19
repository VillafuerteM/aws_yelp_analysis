import pandas as pd
from textblob import TextBlob

# Load your CSV file
data = pd.read_csv('../data/processed/yelp_ihop_sentiment.csv')

# Function to extract nouns and adjectives
def extract_nouns_adjectives(text):
    blob = TextBlob(text)
    nouns = [word for word, tag in blob.tags if tag.startswith('NN')]  # Nouns
    adjectives = [word for word, tag in blob.tags if tag.startswith('JJ')]  # Adjectives
    return " ".join(nouns), " ".join(adjectives)

# Apply the function to each row in the 'text' column
data[['nouns', 'adjectives']] = data['text'].apply(
    lambda x: pd.Series(extract_nouns_adjectives(x))
)

# create a single column with the keywords
data['keywords'] = data['nouns'] + ' ' + data['adjectives']

# Save the results back to a CSV file
data.to_csv('../data/final/yelp_ihop_reviews.csv', index=False)
