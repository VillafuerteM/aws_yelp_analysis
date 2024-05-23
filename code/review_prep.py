# review_prep.py
"""
The objective of this script is to prepare the Yelp data for analysis by performing the following
steps:
1. Load the Yelp review data.
2. Load the Yelp business data.
3. Merge the review and business data.
4. Filter the data to include only reviews for IHOP locations in Florida, Pennsylvania, and
Louisiana from 2015 onwards.
5. Perform sentiment analysis on the reviews.
6. Extract keywords from the reviews.
7. Save the processed data to a CSV file.

The script is divided into several functions to perform each step of the data preparation process.
The main function, `prepare_yelp_data`, orchestrates the execution of these functions in the
correct order.

Inputs:
- Yelp review data file (JSON format)
- Yelp business data file (JSON format)

Outputs:
- Processed Yelp data file (CSV format) with sentiment analysis and keywords extracted

Args:
- review_file (str): Path to the Yelp review data file.
- business_file (str): Path to the Yelp business data file.
- output_file (str): Path to save the processed data.

Usage:
python review_prep.py --review_file <path_to_review_file> --business_file <path_to_business_file> --output_file <output_file_path>

"""

import os
import argparse
import pandas as pd
from utils import etl_yelp_data, analyze_sentiment, extract_key_phrases
import logging

# Configurar logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

def prepare_yelp_data(review_file, business_file, output_file):
    """
    Prepare Yelp data for analysis by performing ETL, sentiment analysis, and keyword extraction.

    Parameters:
    review_file (str): Path to the Yelp review data file.
    business_file (str): Path to the Yelp business data file.
    output_file (str): Path to save the processed data.
    
    Returns:
    None
    """
    try:
        logging.info("Starting data preparation")

        # Perform ETL on Yelp data
        try:
            logging.info("Performing ETL on Yelp data")
            etl_yelp_data(review_file, business_file, 'temp_yelp_data.csv')
        except Exception as e:
            logging.error("Error during ETL process: %s", e)
            raise

        # Perform sentiment analysis
        try:
            logging.info("Performing sentiment analysis using AWS Comprehend")
            df = pd.read_csv('temp_yelp_data.csv')
            df[['Sentiment', 'SentimentScore']] = df['text'].apply(lambda x: pd.Series(analyze_sentiment(x)))
        except Exception as e:
            logging.error("Error during sentiment analysis: %s", e)
            raise

        # Extract keywords
        try:
            logging.info("Extracting key phrases using AWS Comprehend")
            df['KeyPhrases'] = df['text'].apply(lambda x: extract_key_phrases(x))
            df['KeyPhrases'] = df['KeyPhrases'].apply(lambda x: str(x).
                replace('[','').replace(']','').replace(',','').replace("'",''))
        except Exception as e:
            logging.error("Error during keyword extraction: %s", e)
            raise

        # Save the results
        try:
            logging.info(f"Saving processed data to {output_file}")
            df.to_csv(output_file, index=False)
        except Exception as e:
            logging.error("Error saving the processed data: %s", e)
            raise
        
        # Upload the results to S3
        try:
            logging.info(f"Uploading {output_file} to S3 bucket {s3_bucket}")
            s3_object_name = os.path.join(s3_prefix, os.path.basename(output_file))
            upload_to_s3(output_file, s3_bucket, s3_object_name)
        except Exception as e:
            logging.error(f"Error uploading the processed data to S3: {e}")
            raise

        # Clean up temporary files
        try:
            logging.info("Cleaning up temporary files")
            os.remove('temp_yelp_data.csv')
        except Exception as e:
            logging.error("Error cleaning up temporary files: %s", e)
            raise

        logging.info("Data preparation complete")

    except Exception as e:
        logging.error("An error occurred during the data preparation process: %s", e)
        raise

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Prepare Yelp data for analysis.')
    parser.add_argument('--review_file',
        type=str,
        required=True,
        help='Path to the Yelp review data file.')
    parser.add_argument('--business_file',
        type=str,
        required=True,
        help='Path to the Yelp business data file.')
    parser.add_argument('--output_file',
        type=str,
        required=True,
        help='Path to save the processed data.')

    args = parser.parse_args()

    prepare_yelp_data(args.review_file, args.business_file, args.output_file)
