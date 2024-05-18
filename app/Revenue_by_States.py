import streamlit as st
import altair as alt
from backend import Backend

pipe = Backend()

def app():
    select_revsta = st.selectbox('#### Select a Revenue-by-State Report for a date:', pipe.rev_state)
    df_revsta = pipe.get_df(select_revsta,schema='gold')
    df_revsta.rename(columns={'hub_state':'State',
                              'average_revenue':'Average Revenue',
                              'total_revenue': 'Total Revenue'
                              },inplace=True)
    st.write("#### :bar_chart: Revenue by State:")
    st.dataframe(df_revsta, use_container_width=True, hide_index=True)

    state_colors = {
    "SP": "#008080",  
    "RJ": "#6f4a95",  
    "RS": "#4c6ca8",  
    "PR": "#9f8170"   
    }
    bar_chart_avg = alt.Chart(df_revsta, title = 'Average Revenue by State').mark_bar().encode(
        x='Average Revenue',
        y=alt.Y('State', sort='-x'), color=alt.Color('State', scale=alt.Scale(domain=list(state_colors.keys()), range=list(state_colors.values())))
    )
    #st.altair_chart(bar_chart_avg, use_container_width=True)
    
    bar_chart_total = alt.Chart(df_revsta, title='Total Revenue by State').mark_bar().encode(
        x='Total Revenue',
        y=alt.Y('State', sort='-x'), color=alt.Color('State', scale=alt.Scale(domain=list(state_colors.keys()), range=list(state_colors.values())))
    )
    #st.altair_chart(bar_chart_total, use_container_width=True)
    col1, col2 = st.columns(2)

    # Display the charts in the columns
    with col1:
        st.altair_chart(bar_chart_avg, use_container_width=True)

    with col2:
        st.altair_chart(bar_chart_total, use_container_width=True)