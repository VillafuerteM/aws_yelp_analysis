# 1_etl.py
'''
 Objetivo: cargar los datos del data set de Yelp que ya fueron previamente descargados y descomprimidos
 en la carpeta de ../data/raw/ y realizar un proceso de limpieza y transformación de los datos para
 poder ser utilizados por el modelo de Amazon Comprehend.

    INPUTS:
    - yelp_academic_dataset_review.json: archivo JSON con los datos de las reviews de Yelp.
    - yelp_academic_dataset_business.json: archivo JSON con los datos de los negocios de Yelp.

    OUTPUTS:
    - yelp_ihop.csv: archivo CSV con los datos de Yelp limpios y transformados.

Pasos:
1. Cargar los datos.
2. Realizar un left join de los datos.
3. Dar formato de fecha a la columna 'date'.
4. Filtrar los datos de iHOP, del año 2015 en adelante y de los estados de FL, PA y LA.
5. Guardar los datos limpios en un archivo CSV.
 '''

# Importar librerías
import os
import pandas as pd
import json

# lectura de datos
df = pd.read_json('../data/raw/yelp_academic_dataset_review.json', lines=True)
business_data = pd.read_json('../data/raw/yelp_academic_dataset_business.json', lines=True)

# left join de los datos
df = pd.merge(df, business_data, on='business_id', how='left')

# damos formato de fecha a la columna de date
df.loc[:, 'date'] = pd.to_datetime(df['date'])

# nos quedamos solo con los datos de iHOP, del anio 2015 en adelante y de los estados de FL, PA y LA
df_ihop = df[(df['name'] == 'IHOP') & 
             (df['date'].dt.year >= 2015) & 
             (df['state'].isin(['FL', 'PA', 'LA']))]

# guardamos los datos limpios
df_ihop.to_csv('../data/processed/yelp_ihop.csv', index=False)

