import streamlit as st
import altair as alt
from backend import Backend

pipe = Backend()

def app():
    select_revsta = st.selectbox('#### Select a revenue by state table:', pipe.rev_state)
    df_revsta = pipe.get_df(select_revsta,schema='gold')
    st.write("#### Revenue by State:")
    st.dataframe(df_revsta, use_container_width=True, hide_index=True)
    
    bar_chart_sta = alt.Chart(df_revsta).mark_bar().encode(
        x='total_revenue',
        y=alt.Y('hub_state', sort='-x'), color=alt.Color('total_revenue', scale=alt.Scale(scheme='viridis'))
    )
    
    st.altair_chart(bar_chart_sta, use_container_width=True)