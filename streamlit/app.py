# air_quality_dashboard.py
from dotenv import load_dotenv
load_dotenv()
import streamlit as st
import pandas as pd
import plotly.express as px
import os
import psycopg2
from datetime import datetime, timedelta

DB_CONN = os.getenv("DB_URL")  

today = datetime.now().date().strftime("%Y-%m-%d")


@st.cache_data(ttl=3600)  # cache for 1 hour
def load_daily_data():
    conn = psycopg2.connect(DB_CONN)
    query = f"""
        SELECT id, location, sensor_name_units, measurement, date_inserted
        FROM aq_data.daily_measurements
        WHERE date_inserted = '{today}'
        ORDER BY location, sensor_name_units;
    """
    df = pd.read_sql(query, conn)
    conn.close()
    return df

@st.cache_data(ttl=300)  # cache for 5 minutes
def load_data(days: int):
    conn = psycopg2.connect(DB_CONN)
    query = f"""
        SELECT location, sensor_name_units, measurement, date_inserted
        FROM aq_data.daily_measurements
        WHERE date_inserted >= NOW() - INTERVAL '{days} days'
        ORDER BY date_inserted ASC;
    """
    df = pd.read_sql(query, conn)
    conn.close()
    return df

st.set_page_config(page_title="Air Quality Dashboard", layout="wide")

st.title("🌍 Air Quality Dashboard")
st.markdown("Air quality metrics collected from multiple locations.")


daily_df = load_daily_data()


st.header("📊 Daily Air Quality Snapshot (Today)")
with st.expander("Raw Daily Data"):
    st.dataframe(daily_df)

if not daily_df.empty:
    fig_daily = px.bar(
        daily_df,
        x="location",
        y="measurement",
        color="sensor_name_units",
        barmode="group",
        text="measurement",
        title="Today's Air Quality by Location",
    )
    st.plotly_chart(fig_daily, use_container_width=True)
else:
    st.warning("No data found for today.")




days_back = st.sidebar.slider("Select Time Range (days, Trend Graph)", 1, 7, 7, key="days_slider")

df = load_data(days_back)

location_filter = st.sidebar.multiselect(
    "Select Locations (Trend Graph)", df["location"].unique(), default=df["location"].unique(), key="location_select"
)
metric_filter = st.sidebar.multiselect(
    "Select Metrics (Trend Graph)", df["sensor_name_units"].unique(), default=df["sensor_name_units"].unique(), key="metric_select"
)

filtered_df = df[
    (df["location"].isin(location_filter)) &
    (df["sensor_name_units"].isin(metric_filter))
]


st.subheader(f"⏳ Trend Over the Last {days_back} Days")
if filtered_df.empty:
    st.warning("No data available for the selected filters.")
else:
    fig = px.line(
        filtered_df,
        x="date_inserted",
        y="measurement",
        color="sensor_name_units",
        line_group="location",
        facet_col="location",
        facet_col_wrap=2,
        markers=True,
        title=f"Air Quality Trends (last {days_back} days)",
        labels={
        "date_inserted": "Date",
        "measurement": "Air Quality Measurement",
        "sensor_name_units": "Metric",
    }
    )
    fig.for_each_annotation(lambda a: a.update(text=a.text.split("=")[-1]))
    fig.update_layout(height=600)
    st.plotly_chart(fig, use_container_width=True)


    st.subheader(f"🌍 Compare One Metric Across Locations (last {days_back} days)")


focus_metric = st.selectbox(
    "Select one metric to compare across locations",
    filtered_df["sensor_name_units"].unique(),
    key="focus_metric"
)

metric_df = filtered_df[
    (filtered_df["location"].isin(location_filter)) &
    (filtered_df["sensor_name_units"] == focus_metric)
]

if not metric_df.empty:
    fig_focus = px.line(
        metric_df,
        x="date_inserted",
        y="measurement",
        color="location",
        markers=True,
        title=f"{focus_metric} Comparison Across Locations (last {days_back} days)",
        labels={
            "date_inserted": "Date",
            "measurement": f"{focus_metric} Value",
            "location": "Location"
        }
    )
    fig_focus.update_layout(
        height=500,
        xaxis_title="Date",
        yaxis_title=f"{focus_metric} Value",
        legend_title="Location"
    )
    st.plotly_chart(fig_focus, use_container_width=True)
else:
    st.warning(f"No data available for metric: {focus_metric}")

st.markdown("### 📖 Understanding the Metrics")

st.markdown("""
- **PM2.5 (Fine Particulate Matter ≤2.5 μm)**  
  These are very tiny particles that can get deep into the lungs and even enter the bloodstream.  
  - ✅ *Ideal:* Below **12 µg/m³** (EPA standard for good air quality).  
  - ⚠️ *Too High:* Over **35 µg/m³** in 24 hours can be unhealthy, especially for sensitive groups.  
  - 🚨 *Concern:* Long-term exposure to high PM2.5 is linked to respiratory and cardiovascular issues.  

- **PM10 (Coarse Particulate Matter ≤10 μm)**  
  Larger particles like dust, pollen, and smoke. They don’t penetrate as deeply as PM2.5 but can still irritate lungs.  
  - ✅ *Ideal:* Below **50 µg/m³**.  
  - ⚠️ *Too High:* Above **150 µg/m³** (24-hour average) can trigger health advisories.  

- **Ozone (O₃, Ground-level Ozone)**  
  Unlike the protective ozone in the upper atmosphere, ground-level ozone forms from chemical reactions between sunlight, cars, and industry emissions.  
  - ✅ *Ideal:* Below **50 ppb** (parts per billion).  
  - ⚠️ *Too High:* Over **70 ppb** can cause throat irritation, coughing, and worsen asthma.  
""")
