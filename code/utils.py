# Funciones de utilidad para el análisis de reviews de Yelp
import pandas as pd
import os

# funcion para leer y limpiar los datos de Yelp
def etl_yelp_data(input_path='../data/raw/yelp_academic_dataset_review.json', 
                  business_path='../data/raw/yelp_academic_dataset_business.json',
                  output_path='../data/processed/yelp_ihop.csv'):
    '''
    Esta funcion recibe los paths de los archivos de datos de Yelp y realiza un proceso de limpieza y transformación de los datos para
    poder ser utilizados por el modelo de Amazon Comprehend.
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