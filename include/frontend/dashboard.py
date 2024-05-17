#import streamlit as st
import duckdb
from os import getenv
import pandas as pd

config = {
    "allow_unsigned_extensions": "true",
    'dbname': getenv('database_name'),
    'host': getenv('hostname'),
    'password': getenv('password'),
    'port': getenv('port'),
    'user': getenv('username'),
    "allow_unsigned_extensions": "true"
}

con = duckdb.connect()

con.install_extension('postgres')
con.load_extension('postgres')

#con.sql(f"""
#    INSTALL postgres;
#    LOAD postgres;
#    export PGPASSWORD="secret"
#    export PGHOST=localhost
#    export PGUSER=owner
#    export PGDATABASE=mydatabase
#    ATTACH '  
#        dbname={getenv('database_name')}
#        host={getenv('host_url')}
#        port={getenv('port')}  
#        user={getenv('username')}  
#        password={getenv('password')}  
#    ' AS dbfood (TYPE postgres, READ_ONLY);  
#""")

con.sql(f"""
    --INSTALL postgres;
    --LOAD postgres;
    ATTACH '{getenv('external_url')}' AS dbfood (TYPE postgres, READ_ONLY);  
""")
df = con.sql("select * from dbfood.raw.channels").fetchdf()
print(df.head())