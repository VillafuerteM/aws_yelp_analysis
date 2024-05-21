# Funciones de utilidad para el análisis de reviews de Yelp
import pandas as pd
import os
import json
from textblob import TextBlob
import logging
import boto3

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

# Función para leer y limpiar los datos de Yelp
def etl_yelp_data(input_path, business_path, output_path, chunk_size=10000):
    logging.info("Reading Yelp business data from %s", business_path)
    business_data = pd.read_json(business_path, lines=True)

    # Create an empty DataFrame to hold the results
    df_ihop = pd.DataFrame()

    logging.info("Processing Yelp review data in chunks")
    for chunk in pd.read_json(input_path, lines=True, chunksize=chunk_size):
        logging.info("Processing a new chunk")
        # Merge with business data
        chunk = pd.merge(chunk, business_data, on='business_id', how='left')

        # Format date column
        chunk.loc[:, 'date'] = pd.to_datetime(chunk['date'])

        # Filter IHOP data
        chunk_ihop = chunk[(chunk['name'] == 'IHOP') & 
                           (chunk['date'].dt.year >= 2015) & 
                           (chunk['state'].isin(['FL', 'PA', 'LA']))]

        # Append to the main DataFrame
        df_ihop = pd.concat([df_ihop, chunk_ihop])

    logging.info("Saving cleaned data to %s", output_path)
    df_ihop.to_csv(output_path, index=False)


# funcion para realizar análisis de sentimiento
def sentiment_analysis(text):
    """
    Perform sentiment analysis on the given text using TextBlob.

    Parameters:
    text (str): The text to analyze.

    Returns:
    float: The polarity score of the text, ranging from -1 to 1.
    """
    return TextBlob(text).sentiment.polarity


def sentiment_extraction(input_path='../data/processed/yelp_ihop.csv', output_path='../data/processed/yelp_ihop_sentiment.csv'):
    '''
    This function receives the path of a Yelp data file and performs sentiment analysis on the text of the reviews.

    Parameters:
    - input_path (str): The path to the input CSV file containing Yelp data. Default is '../data/processed/yelp_ihop.csv'.
    - output_path (str): The path to save the output CSV file with sentiment analysis results. Default is '../data/processed/yelp_ihop_sentiment.csv'.
    '''
    # Load your CSV file
    logging.info("Loading data from %s", input_path)
    data = pd.read_csv(input_path)

    # Apply sentiment analysis to the 'text' column
    logging.info("Performing sentiment analysis")
    data['sentiment'] = data['text'].apply(sentiment_analysis)

    # Save the results back to a CSV file
    logging.info("Saving sentiment data to %s", output_path)
    data.to_csv(output_path, index=False)

def extract_keywords(input_path='../data/processed/yelp_ihop_sentiment.csv', output_path='../data/final/yelp_ihop_reviews.csv'):
    '''
    This function receives the path of a Yelp data file and extracts keywords from the text of the reviews.

    Parameters:
    - input_path (str): The path to the input CSV file containing Yelp data. Default is '../data/processed/yelp_ihop_sentiment.csv'.
    - output_path (str): The path to save the output CSV file with the extracted keywords. Default is '../data/final/yelp_ihop_reviews.csv'.
    '''
    # Load your CSV file
    logging.info("Loading data from %s", input_path)
    data = pd.read_csv(input_path)

    # Extract nouns and adjectives
    def extract_nouns_adjectives(text):
        """
        Extracts nouns and adjectives from the given text.

        Args:
            text (str): The input text.

        Returns:
            tuple: A tuple containing two strings. The first string contains the extracted nouns, 
                   and the second string contains the extracted adjectives.
        """
        blob = TextBlob(text)
        nouns = [word for word, tag in blob.tags if tag.startswith('NN')]  # Nouns
        adjectives = [word for word, tag in blob.tags if tag.startswith('JJ')]  # Adjectives
        return " ".join(nouns), " ".join(adjectives)

    # Apply the function to each row of the 'text' column
    logging.info("Extracting keywords")
    data[['nouns', 'adjectives']] = data['text'].apply(
        lambda x: pd.Series(extract_nouns_adjectives(x))
    )

    # Create a column with the keywords
    data['keywords'] = data['nouns'] + ' ' + data['adjectives']

    # Save the results back to a CSV file
    logging.info("Saving keyword data to %s", output_path)
    data.to_csv(output_path, index=False)

def analyze_sentiment(text):
    """
    Analyze sentiment of the given text using AWS Comprehend.

    Parameters:
    text (str): The text to analyze.
    
    Returns:
    tuple: A tuple containing the sentiment label and sentiment scores.
    """
    comprehend = boto3.client('comprehend')
    logging.debug("Analyzing sentiment using AWS Comprehend for text: %s", text)
    response = comprehend.detect_sentiment(Text=text, LanguageCode='en')
    sentiment = response['Sentiment']
    sentiment_scores = response['SentimentScore']
    logging.debug("Sentiment: %s, Sentiment Scores: %s", sentiment, sentiment_scores)
    return sentiment, sentiment_scores

def extract_key_phrases(text):
    """
    Extract key phrases from the given text using AWS Comprehend.

    Parameters:
    text (str): The text to analyze.
    
    Returns:
    list: A list of extracted key phrases.
    """
    comprehend = boto3.client('comprehend')
    logging.debug("Extracting key phrases using AWS Comprehend for text: %s", text)
    response = comprehend.detect_key_phrases(Text=text, LanguageCode='en')
    key_phrases = [phrase['Text'] for phrase in response['KeyPhrases']]
    logging.debug("Key phrases: %s", key_phrases)
    return key_phrases