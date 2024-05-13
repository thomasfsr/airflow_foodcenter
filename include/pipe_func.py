import os
import pandas as pd
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine, text
from sqlalchemy.exc import ProgrammingError
import pandera as pa
from dotenv import load_dotenv

load_dotenv()

url = os.getenv('external_url')

from include.infered_schema.schemas import schema_channels, schema_hubs, schema_deliveries,\
      schema_drivers, schema_orders, schema_payments, schema_stores

def create_schema(schema_name:str='raw', url:str=url):

    engine = create_engine(url)
    conn = sessionmaker(bind=engine)
    if conn is None:
        raise ValueError("Connection is not provided.")
    
    with conn() as session:
        try:
            session.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
            session.commit()
        except ProgrammingError as e:
            session.rollback()
            print(f"Error creating schema '{schema_name}': {e}")

def execute_sql_from_file(file_path:str=None, url:str=url):
    
    engine = create_engine(url)
    conn = sessionmaker(bind=engine)
 
    if file_path is None:
        raise ValueError("path is not provided.")
    
    with conn() as session:
        try:
            with open(file_path, 'r') as sql_file:
                sql_statement = text(sql_file.read())
                session.execute(sql_statement)
                session.commit()
        except Exception as e:
            session.rollback()
            print("Error:", e)
        finally:
            session.close()

@pa.check_output(schema=schema_channels, lazy=True, sample=10)
def pandas_read_channels(path:str):
    df = pd.read_csv(path, encoding='ISO-8859-1')
    return df
@pa.check_output(schema=schema_deliveries, lazy=True, sample=10)
def pandas_read_deliveries(path:str):
    df = pd.read_csv(path, encoding='ISO-8859-1')
    return df
@pa.check_output(schema=schema_drivers, lazy=True, sample=10)
def pandas_read_drivers(path:str):
    df = pd.read_csv(path, encoding='ISO-8859-1')
    return df
@pa.check_output(schema=schema_hubs, lazy=True, sample=10)
def pandas_read_hubs(path:str):
    df = pd.read_csv(path, encoding='ISO-8859-1')
    return df
@pa.check_output(schema=schema_orders, lazy=True, sample=10)
def pandas_read_orders(path:str):
    df = pd.read_csv(path, encoding='ISO-8859-1')
    return df
@pa.check_output(schema=schema_payments, lazy=True, sample=10)
def pandas_read_payments(path:str):
    df = pd.read_csv(path, encoding='ISO-8859-1')
    return df
@pa.check_output(schema=schema_stores, lazy=True, sample=10)
def pandas_read_stores(path:str):
    df = pd.read_csv(path, encoding='ISO-8859-1')
    return df

def export_csvs_to_postgresql(data_folder: str = 'data', schema_name: str = 'raw', url: str = url):
    file_reader_mapping = {
        'channels.csv': pandas_read_channels,
        'deliveries.csv': pandas_read_deliveries,
        'drivers.csv': pandas_read_drivers,
        'hubs.csv': pandas_read_hubs,
        'orders.csv': pandas_read_orders,
        'payments.csv': pandas_read_payments,
        'stores.csv': pandas_read_stores
    }

    for file in os.listdir(data_folder):
        file_path = os.path.join(data_folder, file)
        if file in file_reader_mapping:
            reader = file_reader_mapping[file]
            tablename = file.split('.')[0]
            print(file_path)
            df = reader(path=file_path)
            df.to_sql(name=tablename, con=url, schema='raw', if_exists='replace',chunksize=1000)
            print(f'{file} was written in PostgreSQL')
        else:
            print(f"No reader found for file: {file}")

def execute_sql(sql_statement:str=None, url:str=url):
    
    if sql_statement is None:
        raise ValueError("sql is not provided.")
    
    engine = create_engine(url)
    conn = sessionmaker(bind=engine)
    with conn() as session:
        try:
            session.execute(sql_statement)
            session.commit()
        except Exception as e:
            session.rollback()
            print("Error:", e)
        finally:
            session.close()