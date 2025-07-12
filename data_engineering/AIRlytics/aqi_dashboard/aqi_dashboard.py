#######################
# Import libraries
import os
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

aqi_colors = {
    "Good": "#009865",
    "Satisfactory": "#A3C853",
    "Moderate": "#FFD700",
    "Poor": "#FF7E00",
    "Very Poor": "#FF0000",
    "Severe": "#7E0023"
}
aqi_colors_labels = {
    "Good": "🟢",
    "Satisfactory": "🟢",
    "Moderate": "🟡",
    "Poor": "🟠",
    "Very Poor": "🔴",
    "Severe": "🟣"
}

#######################
# CSS styling
# st.markdown("css", unsafe_allow_html=True)

@st.cache_data
def get_filtered_monthly_data(df, year, month):
    return df[(df["year"] == year) & (df["month"] == month)].sort_values(by="aqi", ascending=False)

@st.cache_data
def get_filtered_yearly_data(df, year):
    return df[(df["year"] == year)].sort_values(by="aqi", ascending=False)

@st.cache_data
def compute_monthly_aqi_change(df):
    df = df.copy()
    df = df.sort_values(by=["state", "year", "month"])
    df["aqi_prev"] = df.groupby("state")["aqi"].shift(1)
    df["aqi_change"] = df["aqi"] - df["aqi_prev"]
    df["aqi_pct_change"] = round((df["aqi"] - df["aqi_prev"]) / df["aqi_prev"] * 100, 1)
    return df

@st.cache_data
def compute_aqi_distribution(df):
    df_state_category = (
    df.groupby("state")["aqi_category"]
    .agg(lambda x: x.value_counts().idxmax())  # or .max() for worst
    .reset_index())
    
    category_counts = (
        df_state_category["aqi_category"]
        .value_counts()
        .reset_index(name="count")
        .rename(columns={"index": "aqi_category"})
    )
    return category_counts


#######################
# Sidebar tab navigation

tab_selection = st.sidebar.radio(
    "Select View",
    ["🇮🇳 National View", "🗺️ State-Level View", "🏙️ City-Level View", "🌫️ Pollutant Analysis"]
)

# Load GeoJSON
with open("./utils/india_states.geojson", "r") as f:
    print("Current working directory:", os.getcwd())
    print("Files in cwd:", os.listdir("."))
    india_geo = json.load(f)

#######################
# Tab 1: National View

if tab_selection == "🇮🇳 National View":
    #st.subheader("National Visualizations")
    st.markdown("<h2 style='text-align: center;'>Air Quality Dashboard</h2>", unsafe_allow_html=True)

    
    df_aqi_per_state = run_query(q.get_aqi_data_per_state)
    # map state names to what geojson expects
    df_aqi_per_state = map_state_names(df_aqi_per_state)

    df_aqi_data = run_query(q.get_latest_aqi_data)
    latest_ts = df_aqi_data['record_ts'].max()

    # selected year
    current_year = df_aqi_per_state["year"].max()
    year_list = sorted(df_aqi_per_state["year"].unique().tolist(), reverse=True)
    selected_year = st.sidebar.selectbox("Select Year", year_list)

    # valid months for selected year
    valid_months = sorted(
        df_aqi_per_state[df_aqi_per_state["year"] == selected_year]["month"].unique().tolist(),
        reverse=True
    )
    month_map = {i: calendar.month_name[i] for i in valid_months}
    display_months = [month_map[m] for m in valid_months]
    month_lookup = {v: k for k, v in month_map.items()}  

    # Show month names in dropdown
    selected_month_name = st.sidebar.selectbox("Select Month", display_months)
    selected_month = month_lookup[selected_month_name]  # convert to numeric

    # Filter the data
    df_monthly_aqi_per_state = get_filtered_monthly_data(df_aqi_per_state, selected_year, selected_month)
    df_yearly_aqi_per_state = get_filtered_yearly_data(df_aqi_per_state, selected_year)
    
    color_theme_list = ['blues', 'cividis', 'greens', 'inferno', 'magma', 'plasma', 'reds', 'rainbow', 'turbo', 'viridis']
    selected_color_theme = st.sidebar.selectbox("Select a color theme", color_theme_list)


    def make_choropleth_india(input_df, input_color_theme, selected_month):
        choropleth = px.choropleth(
            input_df,
            geojson=india_geo,
            locations="state_for_map",
            featureidkey="properties.ST_NM",
            color="aqi_category",
            color_discrete_map=aqi_colors,
            hover_name="state",  # optional, shows as title in tooltip
            hover_data={
                "aqi": True,
                "aqi_category": True,
                "state_for_map": False  # hide internal ID
            }
            #color_continuous_scale=input_color_theme,
            #range_color=(0, input_df[input_df.month == selected_month]["aqi"].max()),
            
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
            color=alt.Color('aqi_category:N',
                            legend=None,
                            scale=alt.Scale(domain=list(aqi_colors.keys()), range=list(aqi_colors.values()))),
            stroke=alt.value('black'),
            strokeWidth=alt.value(0.25),
            tooltip=[
                alt.Tooltip('month:N', title='Month'),
                alt.Tooltip(f'{input_x}:O', title='State'),
                alt.Tooltip('aqi_display:N', title='AQI'),
                alt.Tooltip('aqi_category:N', title='Category')
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
    
    def plot_aqi_change_bar(df_selected):
            chart = alt.Chart(df_selected).mark_bar().encode(
                y=alt.Y("state:N", sort="-x", title="State"),
                x=alt.X("aqi_pct_change:Q", title="AQI Change (from prev month)"),
                color=alt.condition(
                    "datum.aqi_pct_change > 0",
                    alt.value("crimson"),   # Worsened
                    alt.value("seagreen")   # Improved
                ),
                #tooltip=["state", "aqi", "aqi_prev", "aqi_pct_change"]
                tooltip=[
                        alt.Tooltip("state", title="State"),
                        alt.Tooltip("aqi", title="Current AQI"),
                        alt.Tooltip("aqi_prev", title="Previous AQI"),
                        alt.Tooltip("aqi_pct_change", title="Change (%)")]
            ).properties(
                width=600,
                height=400,
                title=f"AQI % Change ({calendar.month_abbr[selected_month]} {selected_year})"
            ).configure_axis(
                labelFontSize=12,
                titleFontSize=11
            )
            return chart
    
    def aqi_distribution_pie_chart(category_counts):
        donut_chart = alt.Chart(category_counts).mark_arc(innerRadius=50, outerRadius=120).encode(
        theta=alt.Theta("count:Q"),
        color=alt.Color("aqi_category:N",
            title="AQI Category",
            sort=["Good", "Satisfactory", "Moderate", "Poor", "Very Poor", "Severe"],
            scale=alt.Scale(domain=list(aqi_colors.keys()), range=list(aqi_colors.values()))
        ),
        tooltip=["aqi_category:N", "count:Q"]
        ).properties(
            title=f"AQI Category Breakdown Across States – {selected_year}",
            width=350,
            height=350
        )
        return donut_chart
        
    #######################
    # Dashboard Main Panel
    top_col = st.columns([14])[0]  # or just use st.container()
    with top_col:
        st.markdown("<h4 style='text-align: center;'>AQI Map of India </h4>", unsafe_allow_html=True)
        st.markdown(f"<p style='text-align: center; font-size: 0.9rem; color: grey;'>Last updated: {latest_ts}</p>", unsafe_allow_html=True)

        choropleth = make_choropleth_india(df_monthly_aqi_per_state, selected_color_theme, selected_month)
        # st.plotly_chart(choropleth, use_container_width=True)
        st.plotly_chart(choropleth)
        
        heatmap = make_heatmap(df_yearly_aqi_per_state, 'month', 'state_for_map', 'aqi', selected_color_theme)
        st.altair_chart(heatmap, use_container_width=True)
        
    col = st.columns((4, 4, 4), gap='medium')

    with col[0]:
        st.markdown("#### Top 5: AQI Winners & Losers")
        df_aqi_diff = compute_monthly_aqi_change(df_aqi_per_state)

        # Filter for selected month and year only
        df_selected = df_aqi_diff[
            (df_aqi_diff["year"] == selected_year) & (df_aqi_diff["month"] == selected_month)
        ].dropna(subset=["aqi_change"])  
        # st.write(df_monthly_aqi_per_state)

        # Sort and get top/bottom 5
        top5_worsened = df_selected.nlargest(5, "aqi_pct_change")
        top5_improved = df_selected.nsmallest(5, "aqi_pct_change")
        top5_df = pd.concat([top5_worsened, top5_improved])

        aqi_diff_chart = plot_aqi_change_bar(top5_df)
        st.altair_chart(aqi_diff_chart, use_container_width=True)


    with col[1]:
         st.markdown("<h4 style='text-align: center;'>AQI Distibution(state)</h4>", unsafe_allow_html=True)
         #df_selected_year = df_aqi_per_state[df_aqi_per_state["year"] == selected_year].copy()
         df_selected_year_month = df_monthly_aqi_per_state.copy()
         
         aqi_pie_chart = aqi_distribution_pie_chart(compute_aqi_distribution(df_selected_year_month))
         st.altair_chart(aqi_pie_chart, use_container_width=True)

         with st.expander('About', expanded=True):
            st.write('''
                - Data: [data.gov.in](https://www.data.gov.in/resource/real-time-air-quality-index-various-locations).
                - :orange[**AQI Winners & Losers**]: Top 5 states with highest/lowest AQI % change compared to previous month
                - :orange[** AQI Distribution **]:  AQI category distribution across all states
                - :orange[** Top Polluted States **]:  States ranked from worst to best based on AQI 
                ''')


    with col[2]:
        st.markdown("<h4 style='text-align: center;'>Top Polluted States</h4>", unsafe_allow_html=True)
        df_monthly_aqi_per_state = df_monthly_aqi_per_state.copy()
        df_monthly_aqi_per_state["AQI Level"] = df_monthly_aqi_per_state["aqi_category"].map(
            lambda cat: f"{aqi_colors_labels.get(cat, '')} {cat}"
        )

        st.dataframe(df_monthly_aqi_per_state,
                    column_order=("state_for_map", "aqi", "AQI Level"),
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
                        ),
                        "aqi_category": st.column_config.TextColumn(
                            "AQI Quality",
                        )}
                    )

#######################
# Tab 2: State-Level View

elif tab_selection == "🗺️ State-Level View":
    st.subheader("Coming soon...State-Level Visualizations")

    selected_year = st.sidebar.selectbox("Select Year", ...)
    selected_month = st.sidebar.selectbox("Select Month", ...)
    selected_state = st.sidebar.selectbox("Select State", ...)

#######################
# Tab 3: Pollutant Analysis
elif tab_selection == "🏙️ City-Level View":
    st.subheader("Coming soon...City-Level Visualizations")

    selected_year = st.sidebar.selectbox("Select Year", ...)
    selected_month = st.sidebar.selectbox("Select Month", ...)
    selected_state = st.sidebar.selectbox("Select State", ...)
    selected_city = st.sidebar.selectbox("Select City", ...)

#######################
# Tab 4: Pollutant Analysis

elif tab_selection == "🌫️ Pollutant Analysis":
    st.subheader("Coming soon...Pollutant-Level Analysis")
    selected_year = st.sidebar.selectbox("Select Year", ...)
    selected_month = st.sidebar.selectbox("Select Month", ...)
    selected_state = st.sidebar.selectbox("Select State", ...)
    selected_city = st.sidebar.selectbox("Select City", ...)