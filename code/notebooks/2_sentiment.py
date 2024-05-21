# 2_sentiment.py
"""
Objetivo: realizar un análisis de sentimiento en los datos de Yelp previamente limpiados y transformados.

INPUTS:
- yelp_ihop.csv: archivo CSV con los datos de Yelp limpios y transformados.

OUTPUTS:
- yelp_ihop_sentiment.csv: archivo CSV con los datos de Yelp y una columna adicional con el análisis de sentimiento.

Pasos:
1. Cargar los datos.
2. Realizar análisis de sentimiento en cada review.
3. Guardar los resultados en un archivo CSV.
"""

# Importar librerías
import pandas as pd
from textblob import TextBlob

# Cargar los datos
data = pd.read_csv('../../data/processed/yelp_ihop.csv')

# Funcion para realizar análisis de sentimiento
def sentiment_analysis(text):
    return TextBlob(text).sentiment.polarity

# Aplicamos la función a cada fila de la columna 'text'
data['sentiment'] = data['text'].apply(sentiment_analysis)

# Guardamos los resultados en un archivo CSV
data.to_csv('../../data/processed/yelp_ihop_sentiment.csv', index=False)



