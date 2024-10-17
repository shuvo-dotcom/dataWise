import pandas as pd
from kafka import KafkaProducer
import json
import time
import random

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

weather_data = pd.read_csv('GlobalWeatherRepository.csv')

try:
    while True:
        random_row = weather_data.sample(n=1).to_dict(orient='records')[0]

        producer.send('global_weather', value=random_row)
        print(f"Sent: {random_row}")
        time.sleep(2)

except KeyboardInterrupt:
    print("Producer stopped.")

finally:
    producer.close()