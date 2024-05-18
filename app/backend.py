import duckdb
from os import getenv
import pandas as pd
from dotenv import load_dotenv
from typing import List

class Backend:
    def __init__(self):
        load_dotenv()
        con = duckdb.connect()
        con.install_extension('postgres')
        con.load_extension('postgres')
        con.sql(f"""
            ATTACH '{getenv('external_url')}' AS dbfood (TYPE postgres, READ_ONLY);  
        """)
        query_tables = """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'gold';
        """

        gold_tables = con.sql(query_tables).fetchall()
        gold_tb_list = [table[0] for table in gold_tables]

        self.rankings = [name for name in gold_tb_list if name.startswith('ranking_20_stratified_')]
        self.rev_seg = [name for name in gold_tb_list if name.startswith('revenue_segment_')]
        self.rev_state = [name for name in gold_tb_list if name.startswith('revenue_state_')]
        self.con = con

    def get_df(self,table:str, schema:str)->pd.DataFrame:
        query = f"""
            SELECT * FROM dbfood.{schema}.{table}
            """
        df = self.con.sql(query=query).fetchdf()
        return df
    
    