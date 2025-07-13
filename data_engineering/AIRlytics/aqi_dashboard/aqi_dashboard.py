#######################
# Import libraries
import streamlit as st
import altair as alt
import plotly.express as px
from utils.get_aqi_data import run_query
from utils.state_map import map_state_names
from utils.aqi_charts import *
import utils.queries as q


#######################
# Page configuration
st.set_page_config(
    page_title="AQI",
    page_icon="🇮🇳",
    layout="wide",
    initial_sidebar_state="expanded")

alt.themes.enable("dark")


#######################
# Tab navigation
tab_selection = st.sidebar.radio(
    "Select View",
    ["🇮🇳 AQI Dashboard", "🌫️ Pollutant Analysis"]
)
with st.sidebar.expander('Sources:', expanded=True):
        st.write('''
            - Data: [data.gov.in](https://www.data.gov.in/resource/real-time-air-quality-index-various-locations)
            - GitHub: [AIRlytics](https://github.com/enterthematrix/data-alchemist/tree/dev/data_engineering/AIRlytics)
            ''')

#######################
# Tab 1: National View

if tab_selection == "🇮🇳 AQI Dashboard":
    st.subheader("Air Quality Dashboard")
    aqi_dashboard_subtab = st.radio("Choose a sub-section", ["Maps", "AQI Heatmap", "Trends"], horizontal=True)

    if aqi_dashboard_subtab == "Maps":
        make_choropleth_india(df_aqi_agg_state)
        with st.expander('About', expanded=True):
            st.write('''
                - :orange[**AQI Map**]:  Yearly / Monthly AQI category distribution across all states
                ''')
    elif aqi_dashboard_subtab == "AQI Heatmap":
        make_heatmap(df_aqi_agg_state)
        with st.expander('About', expanded=True):
            st.write('''
                    - :orange[**AQI Heatmap**]: Yearly / Monthly AQI measurements across all states
                    ''')
    elif aqi_dashboard_subtab == "Trends":
        aqi_trends()
        with st.expander('About', expanded=True):
            st.write('''
                    - :orange[**AQI Winners & Losers**]: Top 5 states with highest/lowest AQI % change compared to previous month
                    - :orange[**AQI Distribution**]:  AQI category distribution across all states
                    - :orange[**Top Polluted States**]:  States ranked from worst to best based on AQI
                    ''')
        
#######################
# Tab 2: Pollutant Analysis

elif tab_selection == "🌫️ Pollutant Analysis":
    st.subheader("Pollutant-Level Analysis")
    pollutant_subtab = st.radio("Choose a sub-section", ["Overview", "Trends", "Comparisons", "Maps", "Anomalies"], horizontal=True)

    if pollutant_subtab == "Overview":
        pollutants_overview()
    