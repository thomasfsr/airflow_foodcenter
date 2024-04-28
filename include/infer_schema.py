import pandera
import pandas as pd
import os
from time import time


def infering(input_folder:str = 'data', 
             output_folder: str = "infered_schema"
             ):
    initial = time()
    output_path = f'include/{output_folder}'
    os.makedirs(output_path, exist_ok=True)
    for file in os.listdir(input_folder):
        filename = file.split('.')[0]
        filepath = os.path.join(input_folder,file)
        df = pd.read_csv(filepath,encoding='ISO-8859-1')
        schema = pandera.infer_schema(df)
        output_file_path = os.path.join(output_path, f"schema_{filename}.py")
        with open(output_file_path, "w", encoding='ISO-8859-1') as arquivo:
            arquivo.write(schema.to_script())
    ending = time()
    print(ending-initial)
if __name__ == '__main__':
    infering()