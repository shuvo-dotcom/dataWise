from kafka import KafkaConsumer
import json
import pandas as pd
import time

consumer = KafkaConsumer(
    'global_weather',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='weather_group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

weather_data = pd.DataFrame(columns=[
    'country', 'location_name', 'latitude', 'longitude', 'timezone',
    'last_updated_epoch', 'last_updated', 'temperature_celsius',
    'temperature_fahrenheit', 'condition_text', 'wind_mph', 'wind_kph',
    'wind_degree', 'wind_direction', 'pressure_mb', 'pressure_in',
    'precip_mm', 'precip_in', 'humidity', 'cloud', 'feels_like_celsius',
    'feels_like_fahrenheit', 'visibility_km', 'visibility_miles',
    'uv_index', 'gust_mph', 'gust_kph', 'air_quality_Carbon_Monoxide',
    'air_quality_Ozone', 'air_quality_Nitrogen_dioxide',
    'air_quality_Sulphur_dioxide', 'air_quality_PM2.5', 'air_quality_PM10',
    'air_quality_us-epa-index', 'air_quality_gb-defra-index', 'sunrise',
    'sunset', 'moonrise', 'moonset', 'moon_phase', 'moon_illumination'
])

summary_interval = 60
last_summary_time = time.time()

try:
    print("Consumer started. Listening for messages...")

    for message in consumer:
        data = message.value
        new_data = pd.DataFrame([data])
        weather_data = pd.concat([weather_data, new_data], ignore_index=True)

        if time.time() - last_summary_time >= summary_interval:
            last_summary_time = time.time()
            print("\nSummary of updates consumed:")
            print(weather_data.describe(include='all'))
            print("\nSample of the consumed data:")
            print(weather_data.head())

except KeyboardInterrupt:
    print("Consumer stopped.")

weather_data.to_csv('weather_updates_summary.csv', index=False)
consumer.close()