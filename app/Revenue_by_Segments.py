import streamlit as st
import altair as alt
from backend import Backend

pipe = Backend()

def app():
    select_revseg = st.selectbox('#### Select a Revenue-by-Segments Report for a date:', pipe.rev_seg)
    df_revseg = pipe.get_df(select_revseg,schema='gold')
    st.write("#### Revenue by Segments:")
    st.dataframe(df_revseg, use_container_width=True, hide_index=True)

    bar_chart_seg = alt.Chart(df_revseg).mark_bar().encode(
        x='total_revenue',
        y=alt.Y('store_segment', sort='-x'), 
        color=alt.Color('total_revenue', scale =alt.Scale(scheme='viridis'))
    )

    st.altair_chart(bar_chart_seg, use_container_width=True)