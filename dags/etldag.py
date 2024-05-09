from airflow.decorators import dag, task
from datetime import datetime
from include.extract_with_gdown import GdownDownloader
from include.posgres_raw import Postgres_Pipeline
#import os 
#from dotenv import load_dotenv

@dag(
    #start_date=datetime(2024, 5, 8),
    description='Dag to download data from Google Drive',
    schedule=None,
    catchup=False,
    tags=["gdown"],
)
def gdrive_download():

    @task(task_id= 'calling_class_gdown')
    def downloading():
        downloader = GdownDownloader()
        downloader.download_files()
    
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
                  rank_path:str
                  ):

    @task(task_id= 'postgre_instantiate')
    def instantiate():
        instance = Postgres_Pipeline(url, schema_name='raw')
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
    def create_mvs(instance, mvs_path, rank_path):
        instance.execute_sql_from_file(mvs_path)
        instance.execute_sql_from_file(rank_path)

    instantiated_instance = instantiate()
    schema_created = creating_schema(instantiated_instance)
    silver_gold_created = create_schema_silver_gold(instantiated_instance)
    tables_exported = export_tables(instantiated_instance)
    mv_created = create_mvs(instantiated_instance)

    instantiated_instance >> schema_created
    instantiated_instance >> silver_gold_created >> \
    tables_exported >> mv_created

gdrive_download()
#load_dotenv()
#url = os.getenv('external_url')
url = 'postgresql://ifood_db_user:uYcDYPbDtTynSFHaEa9sGy07OKOK6zhl@dpg-cogk7acf7o1s73811hv0-a.oregon-postgres.render.com/ifood_db'
folder = 'data'
mv_path = '../include/sql/materialized_views.sql'
mv_rank = '../include/sql/rank_view.sql'
postgres_pipe_dag = postgres_pipe(
    url, 
    folder,
    mv_path,
    mv_rank
)