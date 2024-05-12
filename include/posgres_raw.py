import os
import pandas as pd
import time
from sqlalchemy.orm import sessionmaker
from sqlalchemy.schema import CreateSchema
from sqlalchemy import create_engine
from sqlalchemy.exc import ProgrammingError
#import pandera as pa
import polars as pl
#from dotenv import load_dotenv

#from include.infered_schema.schemas import schema_channels, schema_hubs, schema_deliveries,\
#      schema_drives, schema_orders, schema_payments, schema_stores

class Postgres_Pipeline:
    def __init__(self, 
                 url:str, 
                 schema_name:str):
        self.url = url
        self.schema_name = schema_name
        self.conn = self.eng()
    
    def eng(self):
        engine = create_engine(self.url)
        conn = sessionmaker(bind=engine)
        return conn
    
    def create_schema(self, schema_n=None):
        with self.conn() as session:
            if schema_n is None:
                schema_n = self.schema_name
            try:
                session.execute(CreateSchema(schema_n, if_not_exists=True))
                session.commit()
            except ProgrammingError as e:
                session.rollback()
                print(f"Error creating schema '{schema_n}': {e}")
    
    def execute_sql(self, sql_statement):
        with self.conn as session:
            try:
                session.execute(sql_statement)
                session.commit()
            except Exception as e:
                session.rollback()
                print("Error:", e)
            finally:
                session.close()

    def execute_sql_from_file(self, file_path):
        with self.conn as session:
            try:
                with open(file_path, 'r') as sql_file:
                    sql_statement = sql_file.read()
                    session.execute(sql_statement)
                    session.commit()
            except Exception as e:
                session.rollback()
                print("Error:", e)
            finally:
                session.close()

    #@pa.check_output(schema=schema_channels, lazy=True)
    def pandas_read_channels(self, path:str):
        df = pd.read_csv(path, encoding='ISO-8859-1')
        return df

    #@pa.check_output(schema=schema_deliveries, lazy=True)
    def pandas_read_deliveries(self, path:str):
        df = pd.read_csv(path, encoding='ISO-8859-1')
        return df


    #@pa.check_output(schema=schema_drives, lazy=True)
    def pandas_read_drives(self, path:str):
        df = pd.read_csv(path, encoding='ISO-8859-1')
        return df


    #@pa.check_output(schema=schema_hubs, lazy=True)
    def pandas_read_hubs(self, path:str):
        df = pd.read_csv(path, encoding='ISO-8859-1')
        return df


    #@pa.check_output(schema=schema_orders, lazy=True)
    def pandas_read_orders(self, path:str):
        df = pd.read_csv(path, encoding='ISO-8859-1')
        return df


    #@pa.check_output(schema=schema_payments, lazy=True)
    def pandas_read_payments(self, path:str):
        df = pd.read_csv(path, encoding='ISO-8859-1')
        return df


    #@pa.check_output(schema=schema_stores, lazy=True)
    def pandas_read_stores(self, path:str):
        df = pd.read_csv(path, encoding='ISO-8859-1')
        return df

    def export_csvs_to_postgresql(self,data_folder:str):
        start_time = time.time()
        schema_name = self.schema_name
        url = self.url

        readers = [self.pandas_read_channels,
                 self.pandas_read_deliveries,
                 self.pandas_read_drives,
                 self.pandas_read_hubs,
                 self.pandas_read_orders,
                 self.pandas_read_payments,
                 self.pandas_read_stores
                 ]

        for file, reader in zip(os.listdir(data_folder), readers):
                tablename = file.split('.')[0]
                file_path = os.path.join(data_folder, file)
                print(file_path)
                df = reader(path= file_path)
                df = pl.DataFrame._from_pandas(df)
                df.write_database(
                    connection=url,
                    table_name=f'{schema_name}.{tablename}',
                    if_table_exists='replace',
                    engine='adbc'
                )
                print(f'{file} was written in PostgreSQL')
        end_time = time.time()
        elapsed_time = end_time - start_time
        print(f"It took {elapsed_time} seconds")

#load_dotenv()
#url = os.getenv('external_url')
#folder = 'data'

#if __name__ == '__main__':
#    instance = Postgres_Pipeline(url, schema_name='raw')
#    instance.create_schema()
#    instance.create_schema('silver')
#    instance.create_schema('gold')
#    instance.export_csvs_to_postgresql(folder)