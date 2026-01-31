import streamlit as st
import pandas as pd
import plotly.express as px
from sqlalchemy import create_engine, text

# Page Config
st.set_page_config(page_title="Traffic Violations Dashboard",layout="wide")

st.markdown(
    "<h1 style='text-align: center'>Traffic Violation Dashboard</h1>",
    unsafe_allow_html=True
)

# Postrgresql Connection to fetch data and show trends
@st.cache_resource
def get_engine():
    engine = create_engine(
        "postgresql+psycopg2://postgres:password@localhost:5432/your_db",
        pool_pre_ping=True
    )
    return engine

engine = get_engine()

# Fetch Raw data for summary

@st.cache_data
def fetch_raw_data():
    query = """
        SELECT DISTINCT location,
               make,
               accident,
               date_of_stop,
               model,
               violation_type,
               gender,
               race,
               vehicle_category,
               latitude,
               longitude,
               vehicletype
        FROM traffic_violations
        """
    return pd.read_sql(query, engine)

summary_df = fetch_raw_data()

# Summary Statistics
st.header("Summary Statistics - Overall data")
col1, col2, col3, col4, col5 = st.columns([1, 1, 2, 1, 1])

col1.metric("Total Violations", len(summary_df))
col2.metric("Accident Related", summary_df["accident"].sum() if "accident" in summary_df.columns else 0)
col3.metric("High-Risk Zone", summary_df["location"].value_counts().idxmax() if not summary_df.empty else "N/A")
col4.metric("Top Vehicle Make", summary_df["make"].value_counts().idxmax() if "make" in summary_df.columns and not summary_df.empty else "N/A")
col5.metric("Top Vehicle Model", summary_df["model"].value_counts().idxmax() if "model" in summary_df.columns and not summary_df.empty else "N/A")

# Filter options
st.sidebar.header("Filter Options")
filters_df = summary_df[["location","violation_type","gender","race","vehicle_category","latitude","longitude","date_of_stop","make","vehicletype"]].copy()

# Changing datatypes to perform analysis
for col in ["location", "violation_type", "gender", "race", "vehicle_category"]:
    filters_df[col] = filters_df[col].astype(str).str.strip().str.title()

filters_df["date_of_stop"] = pd.to_datetime(filters_df["date_of_stop"],errors="coerce")
filters_df["latitude"] = pd.to_numeric(filters_df["latitude"], errors="coerce")
filters_df["longitude"] = pd.to_numeric(filters_df["longitude"], errors="coerce")
filters_df = filters_df.dropna(subset=["latitude", "longitude"])


# Date filters
start_date = st.sidebar.date_input("Start Date", value=filters_df["date_of_stop"].min())
end_date = st.sidebar.date_input("End Date")

# Categorical filters
location = st.sidebar.multiselect(
    "Location",
    sorted(filters_df["location"].dropna().unique())
)

violation_type = st.sidebar.multiselect(
    "Violation Type",
    sorted(filters_df["violation_type"].dropna().unique())
)

gender = st.sidebar.multiselect(
    "Gender",
    sorted(filters_df["gender"].dropna().unique())
)

race = st.sidebar.multiselect(
    "Race",
    sorted(filters_df["race"].dropna().unique())
)

vehicle_category = st.sidebar.multiselect(
    "Vehicle Category",
    sorted(filters_df["vehicle_category"].dropna().unique())
)
# Latitude range
lat_min, lat_max = float(filters_df["latitude"].min()), float(filters_df["latitude"].max())
latitude_range = st.sidebar.slider(
    "Latitude Range",
    min_value=lat_min,
    max_value=lat_max,
    value=(lat_min, lat_max)
)

# Longitude range
lon_min, lon_max = float(filters_df["longitude"].min()), float(filters_df["longitude"].max())
longitude_range = st.sidebar.slider(
    "Longitude Range",
    min_value=lon_min,
    max_value=lon_max,
    value=(lon_min, lon_max)
)

filtered_df = filters_df.copy()

if start_date and end_date:
    filtered_df = filtered_df[
        (filtered_df["date_of_stop"] >= pd.to_datetime(start_date)) &
        (filtered_df["date_of_stop"] <= pd.to_datetime(end_date))
    ]

if location:
    filtered_df = filtered_df[filtered_df["location"].isin(location)]

if violation_type:
    filtered_df = filtered_df[filtered_df["violation_type"].isin(violation_type)]

if gender:
    filtered_df = filtered_df[filtered_df["gender"].isin(gender)]

if race:
    filtered_df = filtered_df[filtered_df["race"].isin(race)]

if vehicle_category:
    filtered_df = filtered_df[filtered_df["vehicle_category"].isin(vehicle_category)]

filtered_df = filtered_df[
    (filtered_df["latitude"].between(*latitude_range)) &
    (filtered_df["longitude"].between(*longitude_range))
]

tab1, tab2, tab3 = st.tabs(["Trends & Charts", "Distributions & Top Violations", "Geographical Heatmap"])

# Treands and Charts Tab
with tab1:
    st.subheader("Violations Trend Over Time")
    if not filtered_df.empty:
        trend_df = filtered_df.groupby(filtered_df["date_of_stop"].dt.to_period("M")).size().reset_index(name="count")
        trend_df["date_of_stop"] = trend_df["date_of_stop"].astype(str)
        fig_trend = px.line(trend_df, x="date_of_stop", y="count", markers=True, title="Monthly Violation Trend", labels={"date_of_stop": "Month", "count": "Number of Violations"})
        fig_trend.update_layout(xaxis_tickangle=-45,template="plotly_white",height=500)
        st.plotly_chart(fig_trend, use_container_width=True)
    else:
        st.info("No data available for selected filters.")

with tab2:
    st.subheader("Top Vehicle Categories & Vehicle Makes")
    col1, col2 = st.columns(2)

    if not filtered_df.empty:
        with col1:
            top_categories = filtered_df["vehicle_category"].value_counts().head(10)
            fig_category = px.bar(
                x=top_categories.values,
                y=top_categories.index,
                orientation="h",
                title="Top Vehicle Categories",
                labels={"x": "Number of Violations", "y": "Vehicle Category"}
            )
            st.plotly_chart(fig_category, use_container_width=True)

        with col2:
            top_makes = filtered_df["make"].value_counts().head(10)
            fig_make = px.bar(
                x=top_makes.index,
                y=top_makes.values,
                title="Most Frequently Cited Vehicle Makes",
                labels={"x": "Vehicle Make", "y": "Number of Violations"}
            )
            st.plotly_chart(fig_make, use_container_width=True)

    st.subheader("Violation Distribution by Vehicle Type")
    if not filtered_df.empty and "vehicletype" in filtered_df.columns:
        fig_dist = px.histogram(
            filtered_df,
            x="vehicletype",
            color="accident" if "accident" in filtered_df.columns else None,
            barmode="group",
            title="Violations by Vehicle Type",
            labels={"vehicletype": "Vehicle Type", "count": "Number of Violations", "accident": "Accident"}
        )
        st.plotly_chart(fig_dist, use_container_width=True)

# Geographical Heatmap
with tab3:
    st.subheader("Geographical Heatmap of Violations")
    
    # Clean coordinates
    map_df = filtered_df.dropna(subset=["latitude", "longitude"])
    map_df = map_df[(map_df["latitude"] != 0.0) & (map_df["longitude"] != 0.0)]

    # Optional sampling for performance
    if len(map_df) > 5000:
        map_df = map_df.sample(5000, random_state=42)
    
    if not map_df.empty:
        fig_map = px.density_mapbox(
            map_df,
            lat="latitude",
            lon="longitude",
            z="accident" if "accident" in map_df.columns else None,
            radius=10,
            center=dict(lat=map_df["latitude"].mean(), lon=map_df["longitude"].mean()),
            zoom=10,
            mapbox_style="open-street-map",
            title="Traffic Violations Heatmap"
        )
        st.plotly_chart(fig_map, use_container_width=True)
    else:
        st.info("No geographical data available for selected filters.")
