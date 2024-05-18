import streamlit as st
import altair as alt
from backend import Backend
import pandas as pd

pipe = Backend()
def app():
    select_rank = st.selectbox('#### Select a Top 20 Driver Ranking for a date:', pipe.rankings)
    df_rank = pipe.get_df(select_rank,schema='gold').sort_values(by='ranking')
    df_rank.rename(columns={
                            'ranking':'Global Ranking',
                            'driver_id':'Driver ID',
                            'driver_modal':'Modal',
                            'driver_type': 'Type',
                            'sum_of_distance': 'Sum of Distances',
                            'max_distance': 'Max Distance',
                            },inplace=True)
    df_rank['Segmented Ranking'] = range(1,21)
    st.write("#### :trophy: Top 20 Drivers:")
    st.dataframe(df_rank, 
                 use_container_width=True, 
                 hide_index=True,
                 column_order= ['Segmented Ranking',
                            'Driver ID',
                            'Modal',
                            'Type',
                            'Sum of Distances',
                            'Max Distance',
                            'Global Ranking',]
                            )

    df_count = df_rank[['Modal', 'Driver ID']].groupby('Modal').count().reset_index()

    df_dist = df_rank[['Modal', 'Sum of Distances']].groupby('Modal').sum().reset_index()

    set_colors = {"BIKER": "#008080",  "MOTOBOY": "#6f4a95", "FREELANCE": "#008080",  "LOGISTIC OPERATOR": "#6f4a95"}

    bar_chart_c = alt.Chart(df_count, title='Driver Count by Modal').mark_bar().encode(
        x=alt.X('Driver ID', title='Driver Count'),
        y=alt.Y('Modal', sort='-x'), 
        color=alt.Color('Modal', 
                        scale=alt.Scale(domain=list(set_colors.keys()), 
                                        range=list(set_colors.values()))
                        )
    ).properties(
        width=300,
        height=400
    )

    bar_chart_d = alt.Chart(df_dist, title='Sum of distances by Modal').mark_bar().encode(
        x=alt.X('Sum of Distances', title='Total Distance'),
        y=alt.Y('Modal', sort='-x'), 
        color=alt.Color('Modal', 
                        scale=alt.Scale(domain=list(set_colors.keys()), 
                                        range=list(set_colors.values()))
                        )
    ).properties(
        width=300, 
        height=400
    )

    # Create two columns for side-by-side layout
    col1, col2 = st.columns(2)

    # Display the charts in the columns
    with col1:
        st.altair_chart(bar_chart_c, use_container_width=True)

    with col2:
        st.altair_chart(bar_chart_d, use_container_width=True)
    
    #for type
    df_count_t = df_rank[['Type', 'Driver ID']].groupby('Type').count().reset_index()

    df_dist_t = df_rank[['Type', 'Sum of Distances']].groupby('Type').sum().reset_index()

    bar_chart_c_t = alt.Chart(df_count_t, title='Driver Count by Type').mark_bar().encode(
        x=alt.X('Driver ID', title='Driver Count'),
        y=alt.Y('Type', sort='-x'), 
        color=alt.Color('Type', 
                        scale=alt.Scale(domain=list(set_colors.keys()), 
                                        range=list(set_colors.values()))
                        )
    ).properties(
        width=300,
        height=400
    )

    bar_chart_d_t = alt.Chart(df_dist_t, title='Sum of Distances by Type').mark_bar().encode(
        x=alt.X('Sum of Distances', title='Total Distance'),
        y=alt.Y('Type', sort='-x'), 
        color=alt.Color('Type', 
                        scale=alt.Scale(domain=list(set_colors.keys()), 
                                        range=list(set_colors.values()))
                        )
    ).properties(
        width=300, 
        height=400
    )

    # Create two columns for side-by-side layout
    col3, col4 = st.columns(2)

    # Display the charts in the columns
    with col3:
        st.altair_chart(bar_chart_c_t, use_container_width=True)

    with col4:
        st.altair_chart(bar_chart_d_t, use_container_width=True)

    

    