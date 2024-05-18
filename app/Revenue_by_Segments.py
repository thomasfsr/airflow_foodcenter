import streamlit as st
import altair as alt
from backend import Backend

pipe = Backend()

def app():
    select_revseg = st.selectbox('#### Select a Revenue-by-Segments Report for a date:', pipe.rev_seg)
    df_revseg = pipe.get_df(select_revseg,schema='gold')
    df_revseg.rename(columns={'store_segment':'Segment',
                              'average_revenue':'Average Revenue',
                              'total_revenue': 'Total Revenue'
                              },inplace=True)
    st.write("#### :bar_chart: Revenue by Segments:")
    st.dataframe(df_revseg, use_container_width=True, hide_index=True)

    seg_colors = {
    "GOOD": "#008080",  
    "FOOD": "#6f4a95",
    }

    bar_chart_avg = alt.Chart(df_revseg, title='Average Revenue by Segments').mark_bar().encode(
        x='Average Revenue',
        y=alt.Y('Segment', sort='-x'), color=alt.Color('Segment', scale=alt.Scale(domain=list(seg_colors.keys()), range=list(seg_colors.values())))
    )
    #st.altair_chart(bar_chart_avg, use_container_width=True)
    
    bar_chart_total = alt.Chart(df_revseg, title='Total Revenue by Segments').mark_bar().encode(
        x='Total Revenue',
        y=alt.Y('Segment', sort='-x'), color=alt.Color('Segment', scale=alt.Scale(domain=list(seg_colors.keys()), range=list(seg_colors.values())))
    )
    #st.altair_chart(bar_chart_total, use_container_width=True)
    col1, col2 = st.columns(2)

    # Display the charts in the columns
    with col1:
        st.altair_chart(bar_chart_avg, use_container_width=True)

    with col2:
        st.altair_chart(bar_chart_total, use_container_width=True)