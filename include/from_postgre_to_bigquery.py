"""Camada silver:
Desnormalizar dados do postgre para criar um star schema no big query.

Tratar os dados para tirar outliers com Pandera.

"""
from google.api import bigquery
import polars as pl
from dotenv import load_dotenv
import os

load_dotenv()

def query_postgre(query:str, uri:str):
    with open(f'queries/{query}', 'r') as file:
        query = file.read()

    df = pl.read_database_uri(
        query = query,
        uri = uri,
        engine= 'adbc'
    )
    return df

def load_to_bigquery(df: pl.DataFrame, tablename:str, uri:str ):
    df.write_database(table_name=tablename,
                      connection=uri,
                      if_table_exists= 'append',
                      engine='adbc'
                      )

