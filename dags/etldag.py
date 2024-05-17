from airflow.decorators import dag, task

@dag(
    description='Dag to export to postgresql in render.com',
    schedule=None,
    catchup=False,
    tags=["postgre"]
    )
def postgres_pipe():
    from include.extract_with_gdown import gdowner
    from include.pipeline_func import create_schema, \
    execute_sql_from_file, export_csvs_to_postgresql

    @task(task_id = 'download_csvs')
    def downloading():
        gdowner()
    
    @task(task_id = 'create_schemas', trigger_rule="all_done",)
    def creating_schemas():
        create_schema()
        create_schema(schema_name='silver')
        create_schema(schema_name='gold')

    @task(task_id='export_validated_tables', trigger_rule="all_done")
    def export_tables():
        export_csvs_to_postgresql()

    @task(task_id='create_kpis', trigger_rule="all_done")
    def create_mvs():
        execute_sql_from_file('include/sql/creating.sql')

    downloaded = downloading()
    schema_created = creating_schemas()
    tables_exported = export_tables()
    mv_created = create_mvs()

    downloaded >> schema_created
    schema_created >> tables_exported >> mv_created

postgres_pipe()