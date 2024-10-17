import pandas as pd
from kafka import KafkaProducer
import json
import time
import random

# Initialize the Kafka producer
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Load data from the CSV file
weather_data = pd.read_csv('../GlobalWeatherRepository.csv')

try:
    while True:
        # Randomly select a row from the DataFrame
        random_row = weather_data.sample(n=1).to_dict(orient='records')[0]

        # Send the selected data to Kafka topic
        producer.send('global_weather', value=random_row)
        print(f"Sent: {random_row}")

        # Wait for 1 second before sending the next update
        time.sleep(1)

except KeyboardInterrupt:
    print("Producer stopped.")

finally:
    producer.close()