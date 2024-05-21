# 3_keyWords.py
"""
Objetivo: extraer las palabras clave de los datos de Yelp previamente limpiados y transformados.

INPUTS:
- yelp_ihop_sentiment.csv: archivo CSV con los datos de Yelp y una columna adicional con el análisis de sentimiento.

OUTPUTS:
- yelp_ihop_reviews.csv: archivo CSV con los datos de Yelp y una columna adicional con las palabras clave extraídas.

Pasos:
1. Cargar los datos.
2. Definir una función para extraer sustantivos y adjetivos.
3. Aplicar la función a cada fila de la columna 'text'.
4. Crear una columna con las palabras clave.
5. Guardar los resultados en un archivo CSV.
"""

# Importar librerías
import pandas as pd
from textblob import TextBlob

# Cargamos los datos
data = pd.read_csv('../../data/processed/yelp_ihop_sentiment.csv')

# Extraemos sustantivos y adjetivos
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

# Aplicamos la función a cada fila de la columna 'text'
data[['nouns', 'adjectives']] = data['text'].apply(
    lambda x: pd.Series(extract_nouns_adjectives(x))
)

# Creamos una columna con las palabras clave
data['keywords'] = data['nouns'] + ' ' + data['adjectives']

# Guardamos los resultados en un archivo CSV
data.to_csv('../../data/final/yelp_ihop_reviews.csv', index=False)
