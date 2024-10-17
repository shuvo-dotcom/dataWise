import streamlit as st
import pandas as pd
import requests
import matplotlib.pyplot as plt
import seaborn as sns
import time

BASE_URL = "http://127.0.0.1:5000/api"

def fetch_weather_data():
    try:
        response = requests.get(f"{BASE_URL}/weather_data")
        response.raise_for_status()
        return response.json()
    except requests.exceptions.HTTPError as err:
        st.error(f"HTTP error: {err}")
    except requests.exceptions.RequestException as e:
        st.error(f"Request error: {e}")
    return []

st.title("Global Weather Data Dashboard")

data = fetch_weather_data()
if data:
    df = pd.DataFrame(data)
    df['last_updated'] = pd.to_datetime(df['last_updated'])

    st.sidebar.header("Filter Options")

    countries = df['country'].unique()
    selected_country = st.sidebar.selectbox("Select a Country", options=["All"] + list(countries))

    timezones = df['timezone'].unique()
    selected_timezone = st.sidebar.selectbox("Select a Timezone", options=["All"] + list(timezones))

    conditions = df['condition_text'].unique()
    selected_condition = st.sidebar.selectbox("Select Weather Condition", options=["All"] + list(conditions))

    min_temp = df['temperature_celsius'].min()
    max_temp = df['temperature_celsius'].max()
    selected_temp_range = st.sidebar.slider("Select Temperature Range (°C)", min_temp, max_temp, (min_temp, max_temp))

    min_humidity = df['humidity'].min()
    max_humidity = df['humidity'].max()
    selected_humidity_range = st.sidebar.slider("Select Humidity Range (%)", min_humidity, max_humidity, (min_humidity, max_humidity))

    filtered_df = df
    if selected_country != "All":
        filtered_df = filtered_df[filtered_df['country'] == selected_country]
    if selected_timezone != "All":
        filtered_df = filtered_df[filtered_df['timezone'] == selected_timezone]
    if selected_condition != "All":
        filtered_df = filtered_df[filtered_df['condition_text'] == selected_condition]

    filtered_df = filtered_df[
        (filtered_df['temperature_celsius'] >= selected_temp_range[0]) &
        (filtered_df['temperature_celsius'] <= selected_temp_range[1]) &
        (filtered_df['humidity'] >= selected_humidity_range[0]) &
        (filtered_df['humidity'] <= selected_humidity_range[1])
    ]

    st.write("Filtered Weather Data:")
    st.dataframe(filtered_df)

    st.header("Visualizations")

    filtered_df.set_index('last_updated', inplace=True)
    monthly_avg_temp = filtered_df.resample('ME')['temperature_celsius'].mean()
    st.subheader("Monthly Average Temperature")
    st.line_chart(monthly_avg_temp, use_container_width=True)

    avg_temp_by_country = filtered_df.groupby('country')['temperature_celsius'].mean().reset_index()
    st.subheader("Average Temperature by Country")
    st.bar_chart(avg_temp_by_country.set_index('country'), use_container_width=True)

    st.subheader("Temperature vs Humidity")
    plt.figure(figsize=(10, 6))
    plt.scatter(filtered_df['temperature_celsius'], filtered_df['humidity'], alpha=0.6, color='orange')
    plt.xlabel('Temperature (°C)', fontsize=14)
    plt.ylabel('Humidity (%)', fontsize=14)
    plt.title('Temperature vs Humidity', fontsize=16)
    plt.grid(color='lightgrey', linestyle='--', linewidth=0.5)
    st.pyplot(plt)

    st.subheader("Temperature Distribution")
    plt.figure(figsize=(10, 6))
    plt.hist(filtered_df['temperature_celsius'], bins=30, edgecolor='black', color='lightblue')
    plt.xlabel('Temperature (°C)', fontsize=14)
    plt.ylabel('Frequency', fontsize=14)
    plt.title('Temperature Distribution', fontsize=16)
    plt.grid(color='lightgrey', linestyle='--', linewidth=0.5)
    st.pyplot(plt)

    st.subheader("Heatmap of Average Temperature by Country")
    heatmap_data = avg_temp_by_country.set_index('country')
    plt.figure(figsize=(10, 8))
    sns.heatmap(heatmap_data, annot=True, fmt=".1f", cmap='YlGnBu', cbar_kws={'label': 'Temperature (°C)'})
    plt.title('Heatmap of Average Temperature by Country', fontsize=16)
    st.pyplot(plt)

    st.subheader("Humidity Over Time")
    humidity_over_time = filtered_df['humidity'].resample('ME').mean()
    st.line_chart(humidity_over_time, use_container_width=True)

    st.subheader("Weather Conditions Distribution")
    condition_counts = filtered_df['condition_text'].value_counts()
    plt.figure(figsize=(10, 6))
    plt.pie(condition_counts, labels=condition_counts.index, autopct='%1.1f%%', startangle=90, colors=sns.color_palette('pastel'))
    plt.axis('equal')
    plt.title('Weather Conditions Distribution', fontsize=16)
    st.pyplot(plt)

def fetch_live_data():
    try:
        response = requests.get(f"{BASE_URL}/live_updates")
        response.raise_for_status()
        return response.json()
    except requests.exceptions.HTTPError as err:
        st.error(f"HTTP error: {err}")
    except requests.exceptions.RequestException as e:
        st.error(f"Request error: {e}")
    return []

st.title("Live Weather Data")

live_data_display = st.empty()

while True:
    live_data = fetch_live_data()
    if live_data:
        live_df = pd.DataFrame(live_data)
        live_df['last_updated'] = pd.to_datetime(live_df['last_updated'])

        def highlight_max(s):
            is_max = s == s.max()
            return ['background-color: lightgreen' if v else '' for v in is_max]

        styled_df = live_df.style.apply(highlight_max, subset=['temperature_celsius', 'humidity'])

        live_data_display.dataframe(styled_df)

    time.sleep(5)