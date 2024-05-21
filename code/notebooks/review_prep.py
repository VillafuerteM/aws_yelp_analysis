# review_prep.py
"""
The objective of this script is to prepare the Yelp data for analysis by performing the following steps:
1. Load the Yelp review data.
2. Load the Yelp business data.
3. Merge the review and business data.
4. Filter the data to include only reviews for IHOP locations in Florida, Pennsylvania, and Louisiana from 2015 onwards.
5. Perform sentiment analysis on the reviews.
6. Extract keywords from the reviews.
7. Save the processed data to a CSV file.

The script is divided into several functions to perform each step of the data preparation process. The main function, `prepare_yelp_data`, orchestrates the execution of these functions in the correct order.

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

# libraries
import os
import argparse
import pandas as pd
from textblob import TextBlob
from utils import etl_yelp_data, sentiment_analysis, extract_keywords, sentiment_extraction

# function to prepare Yelp data
def prepare_yelp_data(review_file, business_file, output_file):
    """
    Prepare the Yelp data for analysis by performing the following steps:
    1. Load the Yelp review data.
    2. Load the Yelp business data.
    3. Merge the review and business data.
    4. Filter the data to include only reviews for IHOP locations in Florida, Pennsylvania, and Louisiana from 2015 onwards.
    5. Perform sentiment analysis on the reviews.
    6. Extract keywords from the reviews.
    7. Save the processed data to a CSV file.

    Args:
    review_file (str): Path to the Yelp review data file.
    business_file (str): Path to the Yelp business data file.
    output_file (str): Path to save the processed data.

    Returns:
    None
    """
    # Perform ETL on Yelp data
    etl_yelp_data(review_file, business_file, 'temp_yelp_data.csv')

    # Perform sentiment analysis
    sentiment_extraction('temp_yelp_data.csv', 'temp_yelp_sentiment.csv')

    # Extract keywords
    extract_keywords('temp_yelp_sentiment.csv', output_file)

    # Clean up temporary files
    os.remove('temp_yelp_data.csv')
    os.remove('temp_yelp_sentiment.csv')