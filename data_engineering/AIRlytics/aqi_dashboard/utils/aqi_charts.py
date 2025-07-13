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
pollutant_colors = {
        'pm25_avg': '#8c564b',
        'pm10_avg': '#e377c2',
        'so2_avg': '#1f77b4',
        'no2_avg': '#ff7f0e',
        'o3_avg': '#2ca02c',
        'co_avg': '#d62728'
    }

#######################
# Data-prep

base_dir = os.path.dirname(os.path.abspath(__file__))
geojson_path = os.path.join(base_dir,"india_states.geojson")
# Load GeoJSON
with open(geojson_path, "r") as f:
    india_geo = json.load(f)

df_aqi_agg_state = run_query(q.get_aqi_data_per_state)
# map state names to what geojson expects
df_aqi_agg_state = map_state_names(df_aqi_agg_state)

df_aqi_raw_data = run_query(q.get_latest_aqi_data)
latest_ts = df_aqi_raw_data['record_ts'].max()

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

@st.cache_data
def get_month_mappings(valid_months):
    """Returns month name list and lookup dictionaries."""
    month_map = {i: calendar.month_name[i] for i in valid_months}
    display_months = [month_map[m] for m in valid_months]
    month_lookup = {v: k for k, v in month_map.items()}
    return display_months, month_lookup


def show_pollutant_composition_chart(df):
        st.subheader("Pollutant Composition by State")

        available_years = sorted(df['year'].unique())
        selected_year = st.selectbox("Select Year", available_years, index=len(available_years)-1)

        available_months = sorted(df[df['year'] == selected_year]['month'].dropna().unique(), reverse=True)
        # Get month mappings
        display_months, month_lookup = get_month_mappings(available_months)
        display_months = ['All'] + display_months

        selected_month_name = st.selectbox("Select Month", display_months)
        selected_month = None if selected_month_name == 'All' else month_lookup[selected_month_name]

        df_year = df[df['year'] == selected_year].copy()
        if selected_month is not None:
            df_year = df_year[df_year['month'] == selected_month]

        
        pollutant_cols = ['pm25_avg', 'pm10_avg', 'so2_avg', 'no2_avg', 'o3_avg', 'co_avg']
        df_melted = df_year.melt(
            id_vars='state',
            value_vars=pollutant_cols,
            var_name='pollutant',
            value_name='avg_value'
        )

        title_suffix = f"{selected_year}" if selected_month == 'All' else f"{selected_month_name}-{selected_year}"
        chart = alt.Chart(df_melted).mark_bar().encode(
            x=alt.X('state:N', sort='-y', title='State'),
            y=alt.Y('avg_value:Q', stack='normalize', title='Pollutants Composition'),
            color=alt.Color('pollutant:N',
                            scale=alt.Scale(domain=list(pollutant_colors.keys()),
                                            range=list(pollutant_colors.values())),
                            title='Pollutant'),
            tooltip=['state:N', 'pollutant:N', 'avg_value:Q']
        ).properties(
            width=800,
            height=500,
            title=f"Pollutant Composition by State - {title_suffix}"
        )

        st.altair_chart(chart, use_container_width=True)


def make_heatmap(df):
    available_years = sorted(df['year'].unique())
    selected_year = st.selectbox("Select Year", available_years, index=len(available_years)-1)

    available_months = sorted(df[df['year'] == selected_year]['month'].dropna().unique(), reverse=True)
    # Get month mappings
    display_months, month_lookup = get_month_mappings(available_months)
    display_months = ['All'] + display_months

    selected_month_name = st.selectbox("Select Month", display_months)
    selected_month = None if selected_month_name == 'All' else month_lookup[selected_month_name]

    df_heatmap = df[df['year'] == selected_year].copy()
    if selected_month is not None:
        df_heatmap = df_heatmap[df_heatmap['month'] == selected_month]

    title_suffix = f"{selected_year}" if selected_month == 'All' else f"{selected_month_name}-{selected_year}"
    st.subheader(f"AQI Heatmap: {title_suffix}")

    input_y = 'month'
    input_x = 'state_for_map'
    input_color = 'aqi'

    # Keep numeric month for sorting
    df_heatmap['month_num'] = df_heatmap['month']
    df_heatmap['month'] = df_heatmap['month'].apply(lambda x: calendar.month_abbr[int(x)])

    # Create display column for AQI values
    df_heatmap['aqi_display'] = df_heatmap[input_color].apply(
        lambda x: 'NA' if pd.isna(x) else str(int(x))
    )

    # Base heatmap
    heatmap = alt.Chart(df_heatmap).mark_rect().encode(
        y=alt.Y(f'{input_y}:O',
                sort=alt.EncodingSortField(field='month_num', order='descending'),
                axis=alt.Axis(title=f"{df_heatmap['year'].max()}", titleFontSize=18, titlePadding=15, titleFontWeight=900, labelAngle=0)),
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
    text = alt.Chart(df_heatmap).mark_text(
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

    st.altair_chart(chart, use_container_width=True)


def plot_aqi_change_bar(df):
    
    available_years = sorted(df['year'].unique())
    selected_year = st.selectbox("Select Year", available_years, index=len(available_years)-1)

    available_months = sorted(df[df['year'] == selected_year]['month'].dropna().unique(), reverse=True)
    # Get month mappings
    display_months, month_lookup = get_month_mappings(available_months)
    # display_months = ['All'] + display_months

    selected_month_name = st.selectbox("Select Month", display_months)
    selected_month = month_lookup[selected_month_name]

    df_aqi_diff = compute_monthly_aqi_change(df)
    # Filter for selected month and year only
    df_selected = df_aqi_diff[
        (df_aqi_diff["year"] == selected_year) & (df_aqi_diff["month"] == selected_month)
    ].dropna(subset=["aqi_change"])  
    # st.write(df_monthly_aqi_per_state)

    # Sort and get top/bottom 5
    top5_worsened = df_selected.nlargest(5, "aqi_pct_change")
    top5_improved = df_selected.nsmallest(5, "aqi_pct_change")
    top5_df = pd.concat([top5_worsened, top5_improved])

    title_suffix = f"{selected_year}" if selected_month == 'All' else f"{selected_month_name}-{selected_year}"
    st.subheader(f"AQI Winners & Losers: {title_suffix}")
    chart = alt.Chart(top5_df).mark_bar().encode(
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
    st.altair_chart(chart, use_container_width=True)


def aqi_distribution_pie_chart(df):
    available_years = sorted(df['year'].unique())
    selected_year = st.selectbox("Year", available_years, index=len(available_years)-1)

    available_months = sorted(df[df['year'] == selected_year]['month'].dropna().unique(), reverse=True)
    # Get month mappings
    display_months, month_lookup = get_month_mappings(available_months)
    display_months = ['All'] + display_months

    selected_month_name = st.selectbox("Month", display_months)
    selected_month = None if selected_month_name == 'All' else month_lookup[selected_month_name]

    df_selected = df[df['year'] == selected_year].copy()
    if selected_month is not None:
        df_selected = df_selected[df_selected['month'] == selected_month]

    title_suffix = f"{selected_year}" if selected_month == 'All' else f"{selected_month_name}-{selected_year}"
    st.subheader(f"AQI Distibution(state): {title_suffix}")

    category_counts = compute_aqi_distribution(df_selected)
    
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
    st.altair_chart(donut_chart, use_container_width=True)

def aqi_leaderboard(df):
    available_years = sorted(df['year'].unique())
    selected_year = st.selectbox("Year ", available_years, index=len(available_years)-1)

    available_months = sorted(df[df['year'] == selected_year]['month'].dropna().unique(), reverse=True)
    # Get month mappings
    display_months, month_lookup = get_month_mappings(available_months)
    display_months = ['All'] + display_months

    selected_month_name = st.selectbox("Month ", display_months)
    selected_month = None if selected_month_name == 'All' else month_lookup[selected_month_name]

    df_selected = df[df['year'] == selected_year].copy()
    if selected_month is not None:
        df_selected = df_selected[df_selected['month'] == selected_month]

    df_selected["AQI Level"] = df_selected["aqi_category"].map(
        lambda cat: f"{aqi_colors_labels.get(cat, '')} {cat}"
    )
    
    title_suffix = f"{selected_year}" if selected_month == 'All' else f"{selected_month_name}-{selected_year}"
    st.subheader(f"AQI Map: {title_suffix}")
    st.dataframe(df_selected,
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
                        max_value=max(df_selected.aqi),
                    ),
                    "aqi_category": st.column_config.TextColumn(
                        "AQI Quality",
                    )}
                )

def trends():
    col = st.columns((4, 4, 4), gap='medium')
    with col[0]:
        plot_aqi_change_bar(df_aqi_agg_state)
    with col[1]:
        aqi_distribution_pie_chart(df_aqi_agg_state)
    with col[2]:
        aqi_leaderboard(df_aqi_agg_state)
    


def make_choropleth_india(df):
    available_years = sorted(df['year'].unique())
    selected_year = st.selectbox("Select Year", available_years, index=len(available_years)-1)

    available_months = sorted(df[df['year'] == selected_year]['month'].dropna().unique(), reverse=True)
    # Get month mappings
    display_months, month_lookup = get_month_mappings(available_months)
    display_months = ['All'] + display_months

    selected_month_name = st.selectbox("Select Month", display_months)
    selected_month = None if selected_month_name == 'All' else month_lookup[selected_month_name]

    df_year = df[df['year'] == selected_year].copy()
    if selected_month is not None:
        df_year = df_year[df_year['month'] == selected_month]

    title_suffix = f"{selected_year}" if selected_month == 'All' else f"{selected_month_name}-{selected_year}"
    st.subheader(f"AQI Map: {title_suffix}")
        
    choropleth = px.choropleth(
        df_year,
        geojson=india_geo,
        locations="state_for_map",
        featureidkey="properties.ST_NM",
        color="aqi_category",
        color_discrete_map=aqi_colors,
        hover_name="state",  
        hover_data={
            "aqi": True,
            "aqi_category": True,
            "state_for_map": False  
        }
        
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
    st.plotly_chart(choropleth)