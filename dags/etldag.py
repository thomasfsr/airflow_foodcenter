from airflow.decorators import dag, task
#from include.extract_with_google_api import create_data_dir, download_from_gdrive
from include.extract_with_gdown import GdownDownloader
from include.posgres_raw import Postgres_Pipeline
from typing import Dict
import os 
from dotenv import load_dotenv


@dag(
    #start_date=datetime(2024, 5, 8),
    description='Dag to export to postgresql in render.com',
    schedule=None,
    catchup=False,
    tags=["postgre"],
    
)
def postgres_pipe(url: str, 
                  folder:str,
                  mvs_path:str,
                  rank_path:str,
                  schema_name:str,
                  files:Dict
                  ):
    
    @task(task_id= 'create_dir')
    def gdown_instance():
        gdowner = GdownDownloader(files=files,
                                  output_folder='data'
                                  max_retries=3,
                                  retry_delay=1
                                  )

    @task(task_id= 'download_from_gdrive', trigger_rule="all_done")
    def downloading(gdowner):
        gdowner.download_files()

    @task(task_id= 'postgre_instantiate', trigger_rule="all_done",)
    def instantiate():
        instance = Postgres_Pipeline(url, schema_name)
        return instance
    
    @task(task_id = 'create_schema', trigger_rule="all_done")
    def creating_schema(instance):
        instance.create_schema()
    
    @task(task_id= 'create_silver_n_gold', trigger_rule="all_done")
    def create_schema_silver_gold(instance):
        instance.create_schema('silver')
        instance.create_schema('gold')
    
    @task(task_id='export_validated_tables', trigger_rule="all_done")
    def export_tables(instance):
        instance.export_csvs_to_postgresql(folder)

    @task(task_id='create_mviews', trigger_rule="all_done")
    def create_mvs(instance):
        instance.execute_sql_from_file(mvs_path)
        instance.execute_sql_from_file(rank_path)

    #dir_data_created = dir_data()
    gdowner = gdown_instance()
    downloaded = downloading(gdowner)
    instantiated_instance = instantiate()
    schema_created = creating_schema(instantiated_instance)
    silver_gold_created = create_schema_silver_gold(instantiated_instance)
    tables_exported = export_tables(instantiated_instance)
    mv_created = create_mvs(instantiated_instance)

    gdowner >> downloaded
    downloaded >> instantiated_instance >> schema_created >> silver_gold_created >> tables_exported >> mv_created

load_dotenv()
url_e = os.getenv('external_url')
folder = 'data'
mv_path = 'include/sql/materialized_views.sql'
mv_rank = 'include/sql/rank_view.sql'
schema_name = 'raw'

files_list = {
    "channels.csv": "https://drive.google.com/file/d/1wwBnWMfwR5RJZVZULHaWQALrclru-Oyp/view?usp=drive_link",
    "payments.csv": "https://drive.google.com/file/d/1xtchHGwpD8s5_MOVCeMj_cMO6CvYcmS3/view?usp=drive_link",
    "stores.csv": "https://drive.google.com/file/d/1k4pQ3zpNyCUqG2EL1AREGfBPmr5t_ZMK/view?usp=drive_link",
    "orders.csv": "https://drive.google.com/file/d/1kiLBFv6_bR1fv1BahfrTEp5itDut9mVE/view?usp=drive_link",
    "deliveries.csv": "https://drive.google.com/file/d/1ba9-21ppV1Nailp2pJ4RGPjJIJ143_xe/view?usp=drive_link",
    "drives.csv": "https://drive.google.com/file/d/1JZlvYNvD2eVxjrj33s2T54s-itJTvXvI/view?usp=drive_link",
    "hubs.csv": "https://drive.google.com/file/d/1CRRC8K2wHZiL2vdewjb90UT4EcBQQaN5/view?usp=drive_link",
}

postgres_pipe(url=url_e,folder=folder,mvs_path=mv_path,rank_path=mv_rank,schema_name=schema_name,files =files_list)