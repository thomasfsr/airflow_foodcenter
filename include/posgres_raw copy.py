from dotenv import load_dotenv
import os
import pandas as pd
import time
from sqlalchemy.orm import sessionmaker
from sqlalchemy.schema import CreateSchema
from sqlalchemy import create_engine
from sqlalchemy.exc import ProgrammingError
import pandera as pa

from infered_schema.schemas import schema_channels, schema_hubs, schema_deliveries,\
      schema_drives, schema_orders, schema_payments, schema_stores
from infered_schema import schemas

class Postgres:
    def __init__(self, url:str, schema_name:str):
        self.url = url
        self.schema_name = schema_name
        self.engine = create_engine(url)
        self.Session = sessionmaker(bind=self.engine)


    def create_schema(self):
        session = self.Session()
        schema_name = self.schema_name
        try:
            session.execute(CreateSchema(schema_name))
            session.commit()
        except ProgrammingError:
            session.rollback()
            print(f"Schema '{self.schema_name}' already exists.")
        finally:
            session.close()
    
    def execute_sql(self, sql_statement):
        session = self.Session()
        try:
            session.execute(sql_statement)
            session.commit()
        except Exception as e:
            session.rollback()
            print("Error:", e)
        finally:
            session.close()

    @pa.check_output(schema=schema_channels, lazy=True)
    def polars_read(self, path:str):
        #df = pl.read_csv(path, encoding='ISO-8859-1', infer_schema_length=1000)
        df = pd.read_csv(path, encoding='ISO-8859-1')
        return df

    def export_csvs_to_postgresql(self,data_folder:str):
        start_time = time.time()
        schema_name = self.schema_name
        url = self.url

        for file in os.listdir(data_folder):
                tablename = file.split('.')[0]
                file_path = os.path.join(data_folder, file)
                print(file_path)
                df = self.polars_read(path= file_path)
                #df = pl.DataFrame._from_pandas(df)
                df.write_database(
                    connection=url,
                    table_name=f'{schema_name}.{tablename}',
                    if_table_exists='replace',
                    engine='sqlalchemy'
                )
                print(f'{file} was written in PostgreSQL')
        end_time = time.time()
        elapsed_time = end_time - start_time
        print(f"It took {elapsed_time} seconds")

load_dotenv()
url = os.getenv('external_url')
folder = 'data'
raw_schema = 'raw'

if __name__ == '__main__':
    instance = Postgres(url, raw_schema)
    instance.create_schema()
    instance.export_csvs_to_postgresql(folder)