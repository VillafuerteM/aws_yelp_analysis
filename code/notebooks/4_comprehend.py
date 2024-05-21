import pandas as pd
import boto3

df = pd.read_csv('/Users/erwinminor/Downloads/yelp_ihop_reviews.csv')

comprehend = boto3.client('comprehend')

# Función para analizar el sentimiento de una reseña
def analyze_sentiment(text):
    """
    Analyzes the sentiment of the given text using AWS Comprehend.

    Parameters:
    text (str): The text to be analyzed.

    Returns:
    tuple: A tuple containing the sentiment (str) and sentiment scores (dict).
    """

    response = comprehend.detect_sentiment(Text=text, LanguageCode='en')
    sentiment = response['Sentiment']
    sentiment_scores = response['SentimentScore']
    return sentiment, sentiment_scores

# Función para extraer frases clave de una reseña
def extract_key_phrases(text):
    """
    Extracts key phrases from the given text using AWS Comprehend.

    Args:
        text (str): The text to extract key phrases from.

    Returns:
        list: A list of key phrases extracted from the text.
    """
    response = comprehend.detect_key_phrases(Text=text, LanguageCode='en')
    key_phrases = [phrase['Text'] for phrase in response['KeyPhrases']]
    return key_phrases

# Aplicar las funciones a las reseñas y obtener un DataFrame con los resultados
df[['Sentiment', 'SentimentScore']] = df['text'].apply(lambda x: pd.Series(analyze_sentiment(x)))
df['KeyPhrases'] = df['text'].apply(lambda x: extract_key_phrases(x))

# Exportar df a csv
df.to_csv('/Users/erwinminor/Desktop/yelp_ihop_reviews_sentiment_key.csv', index=False)