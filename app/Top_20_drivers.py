import streamlit as st
import altair as alt
from backend import Backend

pipe = Backend()

def app():
    select_rank = st.selectbox('#### Select a ranking table:', pipe.rankings)
    df_rank = pipe.get_df(select_rank,schema='gold').sort_values(by='ranking')
    df_rank.rename(columns={'ranking':'global_ranking'},inplace=True)
    df_rank['stratified_ranking'] = range(1,21)
    df_rank = df_rank[['stratified_ranking','driver_id','driver_modal','driver_type',
                       'sum_of_distance','max_distance','global_ranking']]
    df_rank= df_rank.reset_index(drop=True)
    st.write("#### Top 20 Drivers:")
    st.dataframe(df_rank, use_container_width=True, hide_index=True)
    