from dotenv import load_dotenv
import os
import polars as pl
import time

load_dotenv()

url = os.getenv('external_url')

def export_csvs_to_postgresql(data_folder:str= 'data', external_url:str=url):
    start_time = time.time()

    for file in os.listdir(data_folder):
            tablename = file.split('.')[0]
            file_path = os.path.join(data_folder, file)
            
            df = pl.read_csv(file_path, encoding='ISO-8859-1', infer_schema_length=1000)
            df.write_database(
                connection=external_url,
                table_name=tablename,
                if_table_exists='replace',
                engine='adbc'
            )
            print(f'{file} was written in PostgreSQL')
    end_time = time.time()
    elapsed_time = end_time - start_time
    print(f"It took {elapsed_time} seconds")

external_url = os.getenv('external_url')
folder = 'data'

if __name__ == '__main__':
    export_csvs_to_postgresql(folder, external_url)