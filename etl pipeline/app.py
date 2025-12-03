# app.py - UPDATED WITH YEAR / MONTH / DAY FILTERS AND PERFORMANCE IMPROVEMENTS + DISK CACHING
import streamlit as st
import pandas as pd
import plotly.express as px
from datetime import datetime
import os
from data_extraction import DataExtractor
from data_transformation import DataTransformer

st.set_page_config(page_title="NYC MTA Ridership Dashboard", layout="wide")
st.title("📊 NYC MTA Ridership + Weather Dashboard")

CACHE_FILE = "merged_data.parquet"

@st.cache_data(show_spinner=True)
def load_data():
    extractor = DataExtractor()
    transformer = DataTransformer()

    # Check if cached Parquet exists
    if os.path.exists(CACHE_FILE):
        st.info("✅ Loading cached merged data...")
        merged_df = pd.read_parquet(CACHE_FILE)
        quality_report = transformer.get_quality_report()
    else:
        st.info("⏳ Fetching new data...")

        # Parameters
        start_date = "2023-01-01"
        end_date = "2024-12-31"
        max_records = 600000

        # Extract
        ridership_df = extractor.fetch_ridership_data(
            start_date=start_date,
            end_date=end_date,
            max_records=max_records
        )
        weather_df = extractor.fetch_weather_data(
            start_date=start_date,
            end_date=end_date
        )

        # Merge
        merged_df = transformer.transform_and_merge(ridership_df, weather_df)

        # Save to disk for future runs
        merged_df.to_parquet(CACHE_FILE, index=False)
        quality_report = transformer.get_quality_report()

    # Precompute filter-friendly columns
    merged_df["year"] = merged_df["date"].dt.year
    merged_df["month"] = merged_df["date"].dt.month
    merged_df["month_name"] = merged_df["date"].dt.strftime("%B")
    merged_df["day_name"] = merged_df["date"].dt.day_name()
    merged_df["year_month"] = merged_df["date"].dt.to_period("M").astype(str)

    return merged_df, quality_report

# Load data (cached)
with st.spinner("Loading data..."):
    merged_df, quality_report = load_data()

if merged_df.empty:
    st.error("No data returned from extraction.")
    st.stop()
else:
    st.success(f"Loaded {len(merged_df):,} total records")

# -------------------------------
# SIDEBAR FILTERS
# -------------------------------
st.sidebar.header("Filters")

years_available = sorted(merged_df["year"].unique())
selected_years = st.sidebar.multiselect(
    "Filter by Year", options=years_available, default=years_available
)

months_available = merged_df["month_name"].unique()
selected_months = st.sidebar.multiselect(
    "Filter by Month", options=months_available, default=months_available
)

days = ["Monday","Tuesday","Wednesday","Thursday","Friday","Saturday","Sunday"]
selected_days = st.sidebar.multiselect(
    "Filter by Day of Week", options=days, default=days
)

date_range = st.sidebar.date_input(
    "Select Date Range",
    value=[merged_df["date"].min(), merged_df["date"].max()],
    min_value=merged_df["date"].min().date(),
    max_value=merged_df["date"].max().date()
)

# -------------------------------
# APPLY FILTERS
# -------------------------------
filtered_df = merged_df[
    (merged_df["year"].isin(selected_years)) &
    (merged_df["month_name"].isin(selected_months)) &
    (merged_df["day_name"].isin(selected_days)) &
    (merged_df["date"] >= pd.to_datetime(date_range[0])) &
    (merged_df["date"] <= pd.to_datetime(date_range[1]))
]

# -------------------------------
# SUMMARY STATISTICS
# -------------------------------
st.subheader("📌 Summary Statistics")
filtered_df["ridership"] = pd.to_numeric(filtered_df["ridership"], errors="coerce")

total_records = len(filtered_df)
avg_ridership = filtered_df["ridership"].mean(skipna=True)
max_ridership = filtered_df["ridership"].max(skipna=True)

if total_records > 0:
    date_min = filtered_df["date"].min(skipna=True)
    date_max = filtered_df["date"].max(skipna=True)
    date_span_days = (date_max - date_min).days
else:
    date_span_days = "–"

col1, col2, col3, col4 = st.columns(4)
col1.metric("Filtered Records", f"{total_records:,}")
col2.metric("Avg Ridership", "–" if pd.isna(avg_ridership) else f"{avg_ridership:,.2f}")
col3.metric("Max Ridership", "–" if pd.isna(max_ridership) else f"{max_ridership:,.0f}")
col4.metric("Date Span", date_span_days)

# -------------------------------
# VISUALIZATIONS
# -------------------------------
st.subheader("📈 Ridership Over Time")
fig = px.line(
    filtered_df,
    x="date",
    y="ridership",
    color="year",
    title="Daily Ridership Trend",
    labels={"ridership": "Ridership", "date": "Date"}
)
st.plotly_chart(fig, use_container_width=True)

st.subheader("🌦 Weather Metrics")
fig_weather = px.line(
    filtered_df,
    x="date",
    y=["temperature_mean", "precipitation"],
    title="Temperature & Precipitation Over Time"
)
st.plotly_chart(fig_weather, use_container_width=True)

st.subheader("📅 Ridership by Day of Week")
avg_by_day = filtered_df.groupby("day_name")["ridership"].mean().reindex(days).reset_index()
fig_wd = px.bar(
    avg_by_day, x="day_name", y="ridership",
    title="Average Ridership by Day of Week",
    color="ridership"
)
st.plotly_chart(fig_wd, use_container_width=True)

st.subheader("📋 Sample of Filtered Data")
st.dataframe(filtered_df.head(100), use_container_width=True)

st.subheader("📊 Data Quality Report")
with st.expander("Quality Report"):
    for dataset, metrics in quality_report.items():
        st.markdown(f"### {dataset.upper()}")
        st.table(pd.DataFrame(metrics.items(), columns=["Metric", "Value"]))

st.subheader("💾 Download Filtered Data")
st.download_button(
    "Download CSV",
    filtered_df.to_csv(index=False),
    file_name="filtered_ridership_data.csv",
    mime="text/csv"
)
