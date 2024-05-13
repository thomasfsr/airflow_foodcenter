from airflow.decorators import dag, task
from pendulum import datetime

@dag(
    description='Dag to create new ranking to postgresql in render.com', 
    schedule_interval='@weekly',
    start_date=datetime(2024, 5, 13),
    catchup=False,
    tags=["postgre_new_ranking"]
    )
def ranking_top20_week():
    from include.pipe_func import execute_sql_from_file
    
    @task(task_id = 'new_rank_table')
    def refresh():
        execute_sql_from_file('include/sql/refresh.sql')
        
    new_ranking = refresh()
    new_ranking

ranking_top20_week()
    