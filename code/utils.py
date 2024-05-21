# Funciones de utilidad para el análisis de reviews de Yelp
import pandas as pd
import os
import json
from textblob import TextBlob

# funcion para leer y limpiar los datos de Yelp
def etl_yelp_data(input_path='../data/raw/yelp_academic_dataset_review.json', 
                  business_path='../data/raw/yelp_academic_dataset_business.json',
                  output_path='../data/processed/yelp_ihop.csv'):
    '''
    This function receives the paths of the Yelp data files and performs a cleaning and transformation process on the data to be used by the Amazon Comprehend model.

    Parameters:
    - input_path (str): The path to the Yelp review data file. Default is '../data/raw/yelp_academic_dataset_review.json'.
    - business_path (str): The path to the Yelp business data file. Default is '../data/raw/yelp_academic_dataset_business.json'.
    - output_path (str): The path to save the processed data file. Default is '../data/processed/yelp_ihop.csv'.
    '''
    
    # lectura de datos
    df = pd.read_json(input_path, lines=True)
    business_data = pd.read_json(business_path, lines=True)

    # left join de los datos
    df = pd.merge(df, business_data, on='business_id', how='left')

    # damos formato de fecha a la columna de date
    df.loc[:, 'date'] = pd.to_datetime(df['date'])

    # nos quedamos solo con los datos de iHOP, del anio 2015 en adelante y de los estados de FL, PA y LA
    df_ihop = df[(df['name'] == 'IHOP') & 
                 (df['date'].dt.year >= 2015) & 
                 (df['state'].isin(['FL', 'PA', 'LA']))]

    # guardamos los datos limpios
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


def sentiment_analysis(input_path='../data/processed/yelp_ihop.csv', output_path='../data/processed/yelp_ihop_sentiment.csv'):
    '''
    This function receives the path of a Yelp data file and performs sentiment analysis on the text of the reviews.

    Parameters:
    - input_path (str): The path to the input CSV file containing Yelp data. Default is '../data/processed/yelp_ihop.csv'.
    - output_path (str): The path to save the output CSV file with sentiment analysis results. Default is '../data/processed/yelp_ihop_sentiment.csv'.
    '''
    # Load your CSV file
    data = pd.read_csv(input_path)

    # Apply sentiment analysis to the 'text' column
    data['sentiment'] = data['text'].apply(sentiment_analysis)

    # Save the results back to a CSV file
    data.to_csv(output_path, index=False)