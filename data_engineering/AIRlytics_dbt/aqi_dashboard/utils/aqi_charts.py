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
    'co_avg': '#d62728',
    'nh3_avg': '#9467bd'  
}
# pollutant names
pollutant_cols = ['pm25_avg', 'pm10_avg', 'so2_avg', 'no2_avg', 'o3_avg', 'co_avg',"nh3_avg"]
# pollutant groups
particulate = ["pm25_avg", "pm10_avg"]
gaseous = ["so2_avg", "no2_avg", "o3_avg", "co_avg", "nh3_avg"]

pollutant_group_map = {
    "All": ["pm25_avg", "pm10_avg", "so2_avg", "no2_avg", "o3_avg", "co_avg", "nh3_avg"],
    "Particulate Matter": ["pm25_avg", "pm10_avg"],
    "Gaseous Pollutants": ["so2_avg", "no2_avg", "o3_avg", "co_avg", "nh3_avg"]
}

pollutant_name_map = {
    "pm25_avg": "PM2.5",
    "pm10_avg": "PM10",
    "so2_avg": "SO₂",
    "no2_avg": "NO₂",
    "o3_avg": "O₃",
    "co_avg": "CO",
    "nh3_avg": "NH₃"
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


def show_pollutant_composition_chart(df,selected_year,selected_month,selected_month_name):
        # st.subheader("Pollutant Composition by State")
        df_selected = df[df['year'] == selected_year].copy()
        if selected_month is not None:
            df_selected = df_selected[df_selected['month'] == selected_month]

        
        df_melted = df_selected.melt(
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


def make_heatmap(df):
    available_years = sorted(df['year'].unique())
    selected_year = st.selectbox("Select Year", available_years, index=len(available_years)-1)

    available_months = sorted(df[df['year'] == selected_year]['month'].dropna().unique(), reverse=True)
    # Get month mappings
    display_months, month_lookup = get_month_mappings(available_months)
    display_months = ['All'] + display_months

    selected_month_name = st.selectbox("Select Month", display_months)
    selected_month = None if selected_month_name == 'All' else month_lookup[selected_month_name]

    df_selected = df[df['year'] == selected_year].copy()
    if selected_month is not None:
        df_selected = df_selected[df_selected['month'] == selected_month]

    title_suffix = f"{selected_year}" if selected_month == 'All' else f"{selected_month_name}-{selected_year}"
    st.subheader(f"AQI Heatmap: {title_suffix}")

    input_y = 'month'
    input_x = 'state_for_map'
    input_color = 'aqi'

    # Keep numeric month for sorting
    df_selected['month_num'] = df_selected['month']
    df_selected['month'] = df_selected['month'].apply(lambda x: calendar.month_abbr[int(x)])

    # Create display column for AQI values
    df_selected['aqi_display'] = df_selected[input_color].apply(
        lambda x: 'NA' if pd.isna(x) else str(int(x))
    )

    # Base heatmap
    heatmap = alt.Chart(df_selected).mark_rect().encode(
        y=alt.Y(f'{input_y}:O',
                sort=alt.EncodingSortField(field='month_num', order='descending'),
                axis=alt.Axis(title=f"{df_selected['year'].max()}", titleFontSize=18, titlePadding=15, titleFontWeight=900, labelAngle=0)),
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
    text = alt.Chart(df_selected).mark_text(
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
    else:
        df_selected = df_selected.groupby("state_for_map", as_index=False).agg({
            "aqi": "mean",
            "aqi_category": lambda x: x.mode().iloc[0] if not x.mode().empty else None
        })

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


def national_pollutant_averages(df, selected_year, selected_month):
    df_selected = df[df['year'] == selected_year].copy()
    if selected_month is not None:
        df_selected = df_selected[df_selected['month'] == selected_month]
    avg_values = df_selected[pollutant_cols].mean().reset_index()
    avg_values.columns = ["pollutant", "average"]
    avg_values["pollutant"] = avg_values["pollutant"].str.upper().str.replace("_AVG", "")

    with st.expander("National Average Pollutant Levels"):
        chart = alt.Chart(avg_values).mark_bar(size=35).encode(
            x=alt.X("pollutant:N", title="Pollutant", sort=None),
            y=alt.Y("average:Q", title="Average Level"),
            color=alt.Color("pollutant:N", legend=None),
            tooltip=["pollutant", "average"]
        ).properties(
            width=600,
            height=350,
            title="Average Pollutant Levels Across India"
        )

    st.altair_chart(chart, use_container_width=True)
    

def particulate_vs_gaseous(df, selected_year, selected_month):
    df_selected = df[df['year'] == selected_year].copy()
    if selected_month is not None:
        df_selected = df_selected[df_selected['month'] == selected_month]
    part_avg = df[particulate].sum(axis=1).mean()
    gas_avg = df[gaseous].sum(axis=1).mean()
    particulate_vs_gaseous = pd.DataFrame({
        "Pollutant Category": ["Particulate Matter", "Gaseous Pollutants"],
        "Average Level": [part_avg, gas_avg]
    })

    with st.expander("Particulate vs Gaseous Contribution"):
        pie_chart = alt.Chart(particulate_vs_gaseous).mark_arc(innerRadius=60).encode(
            theta="Average Level:Q",
            color="Pollutant Category:N",
            tooltip=["Pollutant Category", "Average Level"]
        ).properties(
            width=500,
            height=400,
            title="Contribution by Pollutant Category"
        )

    st.altair_chart(pie_chart, use_container_width=True)


def prominent_pollutant_distribution(df,selected_year,selected_month,selected_month_name):
    df_selected = df[df['year'] == selected_year].copy()
    if selected_month is not None:
        df_selected = df_selected[df_selected['month'] == selected_month]

    title_suffix = f"{selected_year}" if selected_month == 'All' else f"{selected_month_name}-{selected_year}"
    # st.subheader(f"Prominent Pollutant: {title_suffix}")
    # Count frequency of each prominent pollutant
    pollutant_counts = df_selected['prominent_pollutant'].value_counts().reset_index()
    pollutant_counts.columns = ['pollutant', 'count']

    fig = px.pie(
        pollutant_counts,
        hole=0.4,
        names='pollutant',
        values='count',
        title='Distribution of Prominent Pollutants Across States',
        color_discrete_sequence=px.colors.qualitative.Set3
    )

    st.plotly_chart(fig, use_container_width=True)
    

def aqi_trends():
    col = st.columns((4, 4, 4), gap='medium')
    with col[0]:
        plot_aqi_change_bar(df_aqi_agg_state)
    with col[1]:
        aqi_distribution_pie_chart(df_aqi_agg_state)
    with col[2]:
        aqi_leaderboard(df_aqi_agg_state)


def pollutants_overview():
    top_col = st.columns([14])[0]
    with top_col:
        df = df_aqi_agg_state.copy()
        available_years = sorted(df['year'].unique())
        selected_year = st.selectbox("Select Year", available_years, index=len(available_years)-1)

        available_months = sorted(df[df['year'] == selected_year]['month'].dropna().unique(), reverse=True)
        # Get month mappings
        display_months, month_lookup = get_month_mappings(available_months)
        display_months = ['All'] + display_months

        selected_month_name = st.selectbox("Select Month", display_months)
        selected_month = None if selected_month_name == 'All' else month_lookup[selected_month_name]
        show_pollutant_composition_chart(df_aqi_agg_state,selected_year,selected_month,selected_month_name)
    col = st.columns((4, 4, 4), gap='medium')
    with col[0]:
        prominent_pollutant_distribution(df_aqi_agg_state,selected_year,selected_month,selected_month_name)
    with col[1]:
        national_pollutant_averages(df_aqi_agg_state, selected_year, selected_month)
    with col[2]:
        particulate_vs_gaseous(df_aqi_agg_state, selected_year, selected_month)


def pollutants_trend(df):
    df_agg = df.copy()
    granularity = st.radio("Select Granularity", ["Yearly", "Monthly"])

    all_states = df_agg['state'].dropna().unique().tolist()
    selected_state = st.selectbox("Select State", ["All"] + sorted(all_states))

    # min_year, max_year = int(df_agg['year'].min()), int(df_agg['year'].max())
    # selected_years = st.slider("Select Year Range", min_year, max_year, (min_year, max_year))
    df_trend = df_agg.copy()

    if selected_state != "All":
        df_trend = df_trend[df_trend['state'] == selected_state]

    # df_trend = df_trend[(df_trend['year'] >= selected_years[0]) & (df_trend['year'] <= selected_years[1])]
    if df_trend.empty:
        st.warning("No data available for the selected state and time range.")
        st.stop()

    if granularity == "Monthly":
        df_trend["time"] = pd.to_datetime(df_trend[["year", "month"]].assign(day=1))
        time_unit = None
        time_format = "%b %Y"
    else:
        df_trend["time"] = pd.to_datetime(df_trend["year"].astype(str), format="%Y")
        time_unit = "year"
        time_format = "%Y"

    df_melted = df_trend.melt(id_vars=["time", "state"], value_vars=pollutant_cols,
                            var_name="pollutant", value_name="concentration")

    line_chart = alt.Chart(df_melted).mark_line().encode(
    x=alt.X(
        "time:T",
        title="Time",
        axis=alt.Axis(format=time_format),
        **({"timeUnit": time_unit} if time_unit else {})
    ),
    y=alt.Y("concentration:Q", title="Avg Concentration", scale=alt.Scale(zero=False)),
        color=alt.Color("pollutant:N", title="Pollutant"),
        tooltip=["time:T", "pollutant:N", "concentration:Q"]
        ).properties(
            width=750,
            height=400,
            title=f"{granularity} Trends of Pollutants" + (f" in {selected_state}" if selected_state != "All" else "")
        )

    st.altair_chart(line_chart, use_container_width=True)


def pollutant_group_area_chart(df):
    st.subheader("Pollutant Type Trends (Particulate vs Gaseous)")

    granularity = st.radio("Select Granularity", ["Yearly", "Monthly"], key="pollutant_group_granularity")

    all_states = df['state'].dropna().unique().tolist()
    selected_state = st.selectbox("Select State", ["All"] + sorted(all_states), key="pollutant_group_state")

    df_group = df.copy()
    if selected_state != "All":
        df_group = df_group[df_group["state"] == selected_state]

    # Convert to datetime
    if granularity == "Monthly":
        df_group["time"] = pd.to_datetime(df_group[["year", "month"]].assign(day=1))
    else:
        df_group["time"] = pd.to_datetime(df_group["year"].astype(str), format="%Y")

    # Group pollutants
    df_group["Particulate Matter"] = df_group["pm25_avg"] + df_group["pm10_avg"]
    df_group["Gaseous Pollutants"] = (
        df_group["so2_avg"] + df_group["no2_avg"] + df_group["o3_avg"] + df_group["co_avg"] + df_group["nh3_avg"]
    )

    df_melted = df_group.melt(
        id_vars=["time", "state"], 
        value_vars=["Particulate Matter", "Gaseous Pollutants"],
        var_name="Pollutant Type", 
        value_name="Concentration"
    )

    area_chart = alt.Chart(df_melted).mark_area(opacity=0.7).encode(
        x=alt.X(
            "time:T",
            title="Time",
            timeUnit="year" if granularity == "Yearly" else "yearmonth",
            axis=alt.Axis(format="%Y" if granularity == "Yearly" else "%b %Y")
        ),
        y=alt.Y("Concentration:Q", stack="normalize", title="Proportional Concentration"),
        color=alt.Color("Pollutant Type:N", scale=alt.Scale(scheme="tableau20")),
        tooltip=["time:T", "Pollutant Type:N", "Concentration:Q"]
    ).properties(
        width=750,
        height=400,
        title=f"{granularity} Composition of Particulate vs Gaseous Pollutants" +
              (f" in {selected_state}" if selected_state != "All" else "")
    )

    st.altair_chart(area_chart, use_container_width=True)


def pollutant_heatmap(df):
    st.subheader("Heatmap of Pollutant Levels by State and Month")

    pollutant_dropdown_map = {v: k for k, v in pollutant_name_map.items()}
    selected_pollutant_label = st.selectbox("Select Pollutant", list(pollutant_dropdown_map.keys()))
    selected_pollutant_col = pollutant_dropdown_map[selected_pollutant_label]

    available_years = sorted(df["year"].dropna().unique())
    selected_year = st.selectbox("Select Year", available_years)

    # Filter and prep data
    df_filtered = df[(df["year"] == selected_year) & df["state"].notna()]
    df_filtered = df_filtered[["state", "month", selected_pollutant_col]].copy()

    # Convert month number to name
    df_filtered["month_name"] = df_filtered["month"].apply(lambda x: calendar.month_abbr[int(x)])
    df_filtered["month_name"] = pd.Categorical(df_filtered["month_name"], categories=list(calendar.month_abbr)[1:], ordered=True)

    # Build heatmap
    heatmap = alt.Chart(df_filtered).mark_rect().encode(
        x=alt.X("month_name:O", title="Month"),
        y=alt.Y("state:N", title="State"),
        color=alt.Color(f"{selected_pollutant_col}:Q", title=f"{selected_pollutant_label} Level", scale=alt.Scale(scheme="reds")),
        tooltip=["state", "month_name", f"{selected_pollutant_col}:Q"]
    ).properties(
        width=700,
        height=500,
        title=f"{selected_pollutant_label} Levels Across States in {selected_year}"
    )

    st.altair_chart(heatmap, use_container_width=True)
