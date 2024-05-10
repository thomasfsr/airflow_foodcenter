from dotenv import load_dotenv
import os
import pandas as pd
import sqlalchemy
import time

load_dotenv()

def export_csvs_to_postgresql(data_folder:str, external_url:str):
    start_time = time.time()    
    engine = sqlalchemy.create_engine(url=external_url, connect_args={'sslmode':'require'})

    for file in os.listdir(data_folder):
            tablename = file.split('.')[0]
            file_path = os.path.join(data_folder, file)
            try:
                df = pd.read_csv(file_path, encoding=None)
            except UnicodeDecodeError:
                df = pd.read_csv(file_path, encoding='ISO-8859-1')

            df.to_sql(
                con=engine,
                name=tablename,
                if_exists='replace',
                index=False
            )
            print(f'{file} was written in PostgreSQL')
    end_time = time.time()
    elapsed_time = end_time - start_time
    print(f"It took {elapsed_time} seconds")

external_url = os.getenv('external_url')
folder = 'data'

export_csvs_to_postgresql(folder, external_url)