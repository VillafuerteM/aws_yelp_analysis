# utils.py
"""
The objective of this script is to provide utility functions for the data preparation process. 
The functions include ETL (Extract, Transform, Load) operations on Yelp data, sentiment analysis
using TextBlob, and keyword extraction from text data. The script also includes functions to
perform sentiment analysis and key phrase extraction using AWS Comprehend.
"""

# Import necessary libraries
import pandas as pd
import os
import json
from textblob import TextBlob
import logging
import boto3

# Configure logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

# ETL process for Yelp data
def etl_yelp_data(input_path, business_path, output_path, chunk_size=10000):
    """
    Perform ETL on Yelp data, filtering for IHOP reviews in specific states from 2015 onwards.

    Parameters:
    input_path (str): Path to the Yelp review data file.
    business_path (str): Path to the Yelp business data file.
    output_path (str): Path to save the processed data file.
    chunk_size (int): Number of lines to process at a time.
    
    Returns:
    None
    """
    try:
        logging.info("Reading Yelp business data from %s", business_path)
        business_data = pd.read_json(business_path, lines=True)

        # Create an empty DataFrame to hold the results
        df_ihop = pd.DataFrame()

        logging.info("Processing Yelp review data in chunks")
        for chunk in pd.read_json(input_path, lines=True, chunksize=chunk_size):
            try:
                logging.info("Processing a new chunk")
                # Merge with business data
                chunk = pd.merge(chunk, business_data, on='business_id', how='left')

                # Replace paragraph breaks with periods in the 'text' column
                chunk['text'] = chunk['text'].str.replace('\n', '. ').str.replace('\r', '')

                # Format date column
                chunk.loc[:, 'date'] = pd.to_datetime(chunk['date'],
                    format='%m/%d/%Y %I:%M:%S %p').dt.strftime('%Y-%m-%d')

                # Filter IHOP data
                chunk_ihop = chunk[(chunk['name'] == 'IHOP') &
                                   (chunk['date'].dt.year >= 2015) &
                                   (chunk['state'].isin(['FL', 'PA', 'LA']))]

                # Append to the main DataFrame
                df_ihop = pd.concat([df_ihop, chunk_ihop])
            except Exception as e:
                logging.error("Error processing chunk: %s", e)
                continue

        logging.info("Saving cleaned data to %s", output_path)
        df_ihop.to_csv(output_path, index=False)
    except Exception as e:
        logging.error("Error in ETL process: %s", e)
        raise

# Sentiment analysis using TextBlob
def sentiment_analysis(text):
    """
    Perform sentiment analysis on the given text using TextBlob.

    Parameters:
    text (str): The text to analyze.
    
    Returns:
    float: The polarity score of the text, ranging from -1 to 1.
    """
    try:
        logging.debug("Analyzing sentiment using TextBlob for text: %s", text)
        return TextBlob(text).sentiment.polarity
    except Exception as e:
        logging.error("Error in sentiment analysis: %s", e)
        raise

# Sentiment extraction from Yelp reviews
def sentiment_extraction(input_path, output_path):
    """
    Extract sentiment from Yelp reviews and save the results.

    Parameters:
    input_path (str): Path to the input CSV file containing Yelp data.
    output_path (str): Path to save the output CSV file with sentiment scores.
    
    Returns:
    None
    """
    try:
        logging.info("Loading data from %s", input_path)
        data = pd.read_csv(input_path)

        logging.info("Performing sentiment analysis")
        data['sentiment'] = data['text'].apply(sentiment_analysis)

        logging.info("Saving sentiment data to %s", output_path)
        data.to_csv(output_path, index=False)
    except Exception as e:
        logging.error("Error in sentiment extraction: %s", e)
        raise

# Keyword extraction from Yelp reviews
def extract_keywords(input_path, output_path):
    """
    Extract keywords (nouns and adjectives) from Yelp reviews and save the results.

    Parameters:
    input_path (str): Path to the input CSV file containing Yelp data.
    output_path (str): Path to save the output CSV file with extracted keywords.
    
    Returns:
    None
    """
    try:
        logging.info("Loading data from %s", input_path)
        data = pd.read_csv(input_path)

        def extract_nouns_adjectives(text):
            """
            Extracts nouns and adjectives from the given text.

            Args:
                text (str): The input text.
            
            Returns:
                tuple: A tuple containing two strings. The first string contains the
                        extracted nouns, and the second string contains the extracted 
                        adjectives.
            """
            try:
                blob = TextBlob(text)
                nouns = [word for word, tag in blob.tags if tag.startswith('NN')]
                adjectives = [word for word, tag in blob.tags if tag.startswith('JJ')]
                return " ".join(nouns), " ".join(adjectives)
            except Exception as e:
                logging.error("Error extracting nouns and adjectives: %s", e)
                raise

        logging.info("Extracting keywords")
        data[['nouns', 'adjectives']] = data['text'].apply(lambda x: pd.Series(extract_nouns_adjectives(x)))
        data['keywords'] = data['nouns'] + ' ' + data['adjectives']

        logging.info("Saving keyword data to %s", output_path)
        data.to_csv(output_path, index=False)
    except Exception as e:
        logging.error("Error in keyword extraction: %s", e)
        raise

# Sentiment analysis using AWS Comprehend
def analyze_sentiment(text):
    """
    Analyze sentiment of the given text using AWS Comprehend.

    Parameters:
    text (str): The text to analyze.
    
    Returns:
    tuple: A tuple containing the sentiment label and sentiment scores.
    """
    try:
        comprehend = boto3.client('comprehend')
        logging.debug("Analyzing sentiment using AWS Comprehend for text: %s", text)
        response = comprehend.detect_sentiment(Text=text, LanguageCode='en')
        sentiment = response['Sentiment']
        sentiment_scores = response['SentimentScore']
        logging.debug("Sentiment: %s, Sentiment Scores: %s", sentiment, sentiment_scores)
        return sentiment, sentiment_scores
    except Exception as e:
        logging.error("Error in AWS Comprehend sentiment analysis: %s", e)
        raise

# Key phrase extraction using AWS Comprehend
def extract_key_phrases(text):
    """
    Extract key phrases from the given text using AWS Comprehend.

    Parameters:
    text (str): The text to analyze.
    
    Returns:
    list: A list of extracted key phrases.
    """
    try:
        comprehend = boto3.client('comprehend')
        logging.debug("Extracting key phrases using AWS Comprehend for text: %s", text)
        response = comprehend.detect_key_phrases(Text=text, LanguageCode='en')
        key_phrases = [phrase['Text'] for phrase in response['KeyPhrases']]
        logging.debug("Key phrases: %s", key_phrases)
        return key_phrases
    except Exception as e:
        logging.error("Error in AWS Comprehend key phrase extraction: %s", e)
        raise

def upload_to_s3(file_path, bucket_name, s3_object_name=None):
    """
    Upload a file to an S3 bucket

    Parameters:
    file_path (str): Path to the file to upload
    bucket_name (str): Name of the S3 bucket
    s3_object_name (str): S3 object name. If not specified, file_path is used

    Returns:
    bool: True if file was uploaded, else False
    """
    if s3_object_name is None:
        s3_object_name = os.path.basename(file_path)

    s3_client = boto3.client('s3')
    try:
        s3_client.upload_file(file_path, bucket_name, s3_object_name)
        logging.info(f"File {file_path} uploaded to {bucket_name}/{s3_object_name}")
        return True
    except Exception as e:
        logging.error(f"Error uploading file to S3: {e}")
        return False
