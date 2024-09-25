
def postgres_pipe():
    from extract_with_gdown import gdowner
    from pipeline_func import create_schema, \
    execute_sql_from_file, export_csvs_to_postgresql

    # gdowner() # Não precisa fazer download novamente dos CSV.

    # def creating_schemas():
    # create_schema()
    # create_schema(schema_name='silver')
    # create_schema(schema_name='gold')
 
    # def export_tables():
    # export_csvs_to_postgresql()

    # def create_mvs():
    execute_sql_from_file('include/sql/creating.sql')

    # downloaded = downloading()
    # schema_created = creating_schemas()
    # tables_exported = export_tables()
    # mv_created = create_mvs()

    # downloaded >> schema_created
    # schema_created >> tables_exported >> mv_created

postgres_pipe()