#######################
# Import libraries
import streamlit as st
import pandas as pd
import altair as alt
import plotly.express as px
from utils.get_aqi_data import run_query
from utils.state_map import map_state_names
import utils.queries as q
import json
import calendar

#######################
# Page configuration
st.set_page_config(
    page_title="AQI",
    page_icon="🇮🇳",
    layout="wide",
    initial_sidebar_state="expanded")

alt.themes.enable("dark")

#######################
# CSS styling
st.markdown("""
<style>

[data-testid="block-container"] {
    padding-left: 2rem;
    padding-right: 2rem;
    padding-top: 1rem;
    padding-bottom: 0rem;
    margin-bottom: -7rem;
}

[data-testid="stVerticalBlock"] {
    padding-left: 0rem;
    padding-right: 0rem;
}

[data-testid="stMetric"] {
    background-color: #393939;
    text-align: center;
    padding: 15px 0;
}

[data-testid="stMetricLabel"] {
  display: flex;
  justify-content: center;
  align-items: center;
}

[data-testid="stMetricDeltaIcon-Up"] {
    position: relative;
    left: 38%;
    -webkit-transform: translateX(-50%);
    -ms-transform: translateX(-50%);
    transform: translateX(-50%);
}

[data-testid="stMetricDeltaIcon-Down"] {
    position: relative;
    left: 38%;
    -webkit-transform: translateX(-50%);
    -ms-transform: translateX(-50%);
    transform: translateX(-50%);
}

</style>
""", unsafe_allow_html=True)


@st.cache_data
def get_filtered_monthly_data(df, year, month):
    return df[(df["year"] == year) & (df["month"] == month)].sort_values(by="aqi", ascending=False)

@st.cache_data
def get_filtered_yearly_data(df, year):
    return df[(df["year"] == year)].sort_values(by="aqi", ascending=False)

#######################
# Load GeoJSON
with open("./utils/india_states.geojson", "r") as f:
    india_geo = json.load(f)

df_aqi_per_state = run_query(q.get_aqi_data_per_state)
# map state names to what geojson expects
df_aqi_per_state = map_state_names(df_aqi_per_state)

df_aqi_data = run_query(q.get_latest_aqi_data)
latest_ts = df_aqi_data['record_ts'].max()
# st.caption(f"Last updated: {latest_ts}")

#######################
# Sidebar
with st.sidebar:
    st.title('🇮🇳 Live Air Quality Index (AQI)')

    # selected year
    current_year = df_aqi_per_state["year"].max()
    year_list = sorted(df_aqi_per_state["year"].unique().tolist(), reverse=True)
    selected_year = st.selectbox('Select a year', year_list)

    # valid months for selected year
    valid_months = sorted(
        df_aqi_per_state[df_aqi_per_state["year"] == selected_year]["month"].unique().tolist(),
        reverse=True
    )
    month_map = {i: calendar.month_name[i] for i in valid_months}
    display_months = [month_map[m] for m in valid_months]
    month_lookup = {v: k for k, v in month_map.items()}  

    # Show month names in dropdown
    selected_month_name = st.selectbox("Select a month", display_months)
    selected_month = month_lookup[selected_month_name]  # convert to numeric

    # Filter the data
    df_monthly_aqi_per_state = get_filtered_monthly_data(df_aqi_per_state, selected_year, selected_month)
    df_yearly_aqi_per_state = get_filtered_yearly_data(df_aqi_per_state, selected_year)
    
    color_theme_list = ['blues', 'cividis', 'greens', 'inferno', 'magma', 'plasma', 'reds', 'rainbow', 'turbo', 'viridis']
    selected_color_theme = st.selectbox('Select a color theme', color_theme_list)

def make_choropleth_india(input_df, input_color_theme, selected_month):
    choropleth = px.choropleth(
        input_df,
        geojson=india_geo,
        locations="state_for_map",
        featureidkey="properties.ST_NM",
        color="aqi",
        color_continuous_scale=input_color_theme,
        range_color=(0, input_df[input_df.month == selected_month]["aqi"].max()),
        
    )
    
    choropleth.update_geos(
        fitbounds="locations", visible=False
    )
    choropleth.update_layout(
        template='plotly_dark',
        plot_bgcolor='rgba(0, 0, 0, 0)',
        paper_bgcolor='rgba(0, 0, 0, 0)',
        margin=dict(l=0, r=0, t=0, b=0),
        height=700

    )
    #choropleth.update_layout(margin={"r":0, "t":0, "l":0, "b":0})
    return choropleth

def make_heatmap(input_df, input_y, input_x, input_color, input_color_theme):
    input_df = input_df.copy()

    # Keep numeric month for sorting
    input_df['month_num'] = input_df['month']
    input_df['month'] = input_df['month'].apply(lambda x: calendar.month_abbr[int(x)])

    # Create display column for AQI values
    input_df['aqi_display'] = input_df[input_color].apply(
        lambda x: 'NA' if pd.isna(x) else str(int(x))
    )

    # Base heatmap
    heatmap = alt.Chart(input_df).mark_rect().encode(
        y=alt.Y('month:O',
                sort=alt.EncodingSortField(field='month_num', order='descending'),
                axis=alt.Axis(title=f"{input_df['year'].max()}", titleFontSize=18, titlePadding=15, titleFontWeight=900, labelAngle=0)),
        x=alt.X(f'{input_x}:O',
                sort=alt.EncodingSortField(field=input_color, op='max', order='descending'),
                axis=alt.Axis(title="States", titleFontSize=18, titlePadding=15, titleFontWeight=900)),
        color=alt.Color(f'{input_color}:Q',
                        legend=None,
                        scale=alt.Scale(scheme=input_color_theme)),
        stroke=alt.value('black'),
        strokeWidth=alt.value(0.25),
        tooltip=[
            alt.Tooltip('month:N', title='Month'),
            alt.Tooltip(f'{input_x}:O', title='State'),
            alt.Tooltip('aqi_display:N', title='AQI')
        ]
    )
    # Text labels
    text = alt.Chart(input_df).mark_text(
        baseline='middle',
        fontSize=12,
        fontWeight='bold'
    ).encode(
        y=alt.Y('month:O', sort=alt.EncodingSortField(field='month_num', order='descending')),
        x=alt.X(f'{input_x}:O'),
        text='aqi_display:N',
        color=alt.condition(
            alt.datum[input_color] > 200,
            alt.value('white'),
            alt.value('black')
        )
    )
    # Combine heatmap and text
    chart = (heatmap + text).properties(width=900).configure_axis(
        labelFontSize=12,
        titleFontSize=12
    )

    return chart
#######################
# Dashboard Main Panel
col = st.columns((2, 8, 3.5), gap='medium')

with col[0]:
    st.markdown("#### High's / Low's")

with col[1]:
    st.markdown("<h4 style='text-align: center;'>AQI Map of India </h4>", unsafe_allow_html=True)
    st.markdown(f"<p style='text-align: center; font-size: 0.9rem; color: grey;'>Last updated: {latest_ts}</p>", unsafe_allow_html=True)

    choropleth = make_choropleth_india(df_monthly_aqi_per_state, selected_color_theme, selected_month)
    # st.plotly_chart(choropleth, use_container_width=True)
    st.plotly_chart(choropleth)
    
    heatmap = make_heatmap(df_yearly_aqi_per_state, 'month', 'state_for_map', 'aqi', selected_color_theme)
    st.altair_chart(heatmap, use_container_width=True)
    # st.altair_chart(heatmap)
    
with col[2]:
    st.markdown("<h4 style='text-align: center;'>Top Polluted States</h4>", unsafe_allow_html=True)

    st.dataframe(df_monthly_aqi_per_state,
                 column_order=("state_for_map", "aqi"),
                 hide_index=True,
                 width=None,
                 column_config={
                    "state_for_map": st.column_config.TextColumn(
                        "States",
                    ),
                    "aqi": st.column_config.ProgressColumn(
                        "AQI",
                        format="%f",
                        min_value=0,
                        max_value=max(df_aqi_per_state.aqi),
                     )}
                 )
    
    with st.expander('About', expanded=True):
        st.write('''
            - Data: [data.gov.in](https://www.data.gov.in/resource/real-time-air-quality-index-various-locations).
            - :orange[**Gains/Losses**]: states with highest/lowest AQI for selected months
            - :orange[** AQI % change **]: percentage of AQI changed with compared to last month
            ''')
