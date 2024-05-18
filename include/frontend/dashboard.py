import streamlit as st
import duckdb
from os import getenv
import pandas as pd
import altair as alt

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

rankings = [name for name in gold_tb_list if name.startswith('ranking_20_stratified_')]
rev_seg = [name for name in gold_tb_list if name.startswith('revenue_segment_')]
rev_state = [name for name in gold_tb_list if name.startswith('revenue_state_')]

def get_df(table:str, schema:str)->pd.DataFrame:
    query = f"""
        SELECT * FROM dbfood.{schema}.{table}
        """
    df = con.sql(query=query).fetchdf()
    return df

select_rank = st.selectbox('#### Select a ranking table:', rankings)
df_rank = get_df(select_rank,schema='gold').sort_values(by='ranking')
df_rank.rename(columns={'ranking':'global_ranking'},inplace=True)
df_rank['stratified_ranking'] = range(1,21)
df_rank = df_rank[['stratified_ranking','driver_id','driver_modal','driver_type',
                   'sum_of_distance','max_distance','global_ranking']]
df_rank= df_rank.reset_index(drop=True)
st.write("#### Top 20 Drivers:")
st.dataframe(df_rank)

select_revseg = st.selectbox('#### Select a revenue by segment table:', rev_seg)
df_revseg = get_df(select_revseg,schema='gold')
st.write("#### Revenue by Segments:")
st.dataframe(df_revseg)

bar_chart_seg = alt.Chart(df_revseg).mark_bar().encode(
    x='total_revenue',
    y=alt.Y('store_segment', sort='-x'), color=alt.Color('total_revenue', scale =alt.Scale(scheme='viridis'))
)

st.altair_chart(bar_chart_seg, use_container_width=True)

select_revsta = st.selectbox('#### Select a revenue by state table:', rev_state)
df_revsta = get_df(select_revsta,schema='gold')
st.write("#### Revenue by State:")
st.dataframe(df_revsta)

bar_chart_sta = alt.Chart(df_revsta).mark_bar().encode(
    x='total_revenue',
    y=alt.Y('hub_state', sort='-x'), color=alt.Color('total_revenue', scale=alt.Scale(scheme='viridis'))
)
text = bar_chart_sta.mark_text(
    align='left',
    baseline='middle',
    dx=3,  # Adjust the horizontal position of the text labels
    fontSize=14  # Set the font size of the text labels
).encode(
    text='total_revenue'
)

bar_chart_with_labels = (bar_chart_sta + text)

st.altair_chart(bar_chart_with_labels, use_container_width=True)
