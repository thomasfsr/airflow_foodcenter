from airflow.decorators import dag, task
from datetime import datetime
from include.extract_with_gdown import GdownDownloader

@dag(
    start_date=datetime(2024, 4, 26),
    description='Dag to download data from Google Drive',
    schedule="@daily",
    catchup=False,
    #doc_md=__doc__,
    #default_args={"owner": "Astro", "retries": 3},
    tags=["gdown"],
)

def pipeline_gdrive_download():

    @task(task_id= 'calling_class_gdown')
    def instantiate():
        downloader = GdownDownloader()
        downloader.download_files()
        
pipeline_gdrive_download()