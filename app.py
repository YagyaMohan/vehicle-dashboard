import streamlit as st
from kafka import KafkaConsumer
import json
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import time

st.set_page_config(page_title="Real-Time Vehicle Tracking", layout="wide")
st.title("🚦 Real-Time Vehicle Movement Dashboard")

# Kafka Consumer
consumer = KafkaConsumer(
    'vehicle_tracking',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='latest',
    enable_auto_commit=True,
    group_id="streamlit-dashboard",
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

# Placeholders for live updates
map_placeholder = st.empty()
gauge_placeholder = st.empty()
chart_placeholder = st.empty()
table_placeholder = st.empty()

data = []

# UI Loop
while True:
    for msg in consumer:
        event = msg.value
        data.append(event)
        df = pd.DataFrame(data)

        # --- LIVE MAP ---
        fig_map = px.scatter_mapbox(
            df,
            lat="lat",
            lon="lon",
            color="speed",
            size_max=15,
            zoom=11,
            height=400,
            hover_name="car_id"
        )
        fig_map.update_layout(mapbox_style="open-street-map")
        map_placeholder.plotly_chart(
            fig_map, 
            use_container_width=True, 
            key=f"map_{event['car_id']}_{event['timestamp']}"
        )

        # --- SPEED GAUGE FOR LAST VEHICLE ---
        latest_speed = event['speed']
        gauge = go.Figure(go.Indicator(
            mode="gauge+number",
            value=latest_speed,
            title={'text': f"Speed of {event['car_id']}"},
            gauge={'axis': {'range': [0, 150]}}
        ))
        gauge_placeholder.plotly_chart(
            gauge, 
            use_container_width=True, 
            key=f"gauge_{event['car_id']}_{event['timestamp']}"
        )

        # --- LINE CHART (Speed History) ---
        fig_chart = px.line(
            df.tail(100),
            x="timestamp",
            y="speed",
            color="car_id",
            title="📈 Speed Trend (Last 100 events)"
        )
        chart_placeholder.plotly_chart(
            fig_chart, 
            use_container_width=True, 
            key=f"chart_{event['car_id']}_{event['timestamp']}"
        )

        # --- LIVE TABLE ---
        table_placeholder.dataframe(
            df.tail(20), 
            height=300,
            key=f"table_{event['car_id']}_{event['timestamp']}"
        )

        time.sleep(0.3)
