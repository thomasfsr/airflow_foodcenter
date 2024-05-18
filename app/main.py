import streamlit as st
from streamlit_option_menu import option_menu
import Revenue_by_Segments, Revenue_by_States, Top_20_drivers, tasks


st.set_page_config(
    page_title='Reports',
    layout='wide'
)

#st.title('Delivery Center: Food & Goods orders in Brazil')

class MultiApp:

    def __init__(self):
        self.apps = []

    def add_app(self, title, func):

        self.apps.append({
            "title": title,
            "function": func
        })

    def run():
        with st.sidebar:        
            app = option_menu(
                menu_title='Reports',
                options=['Task','Top 20 Drivers','Revenue by Segments', 'Revenue by States'],
                icons=['list-task','trophy-fill','bar-chart-line-fill','bar-chart-line-fill',],
                menu_icon='collection',
                default_index=0
                )
        
        if app == 'Task':
            tasks.app()
        if app == "Top 20 Drivers":
            Top_20_drivers.app()    
        if app == "Revenue by Segments":
            Revenue_by_Segments.app()        
        if app == 'Revenue by States':
            Revenue_by_States.app()

    run()