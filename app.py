import threading
from flask import Flask, jsonify
from flask_restful import Api, Resource
from kafka import KafkaConsumer
import json
from pymongo import MongoClient

app = Flask(__name__)
api = Api(app)

# MongoDB connection
client = MongoClient('mongodb://localhost:27017/')
db = client.WeatherData
collection = db.weather_data


kafka_messages = []

def consume_messages():
    kafka_consumer = KafkaConsumer(
        'global_weather',
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='weather_group',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )
    
    for message in kafka_consumer:
        kafka_messages.append(message.value)
        
        if len(kafka_messages) > 100:
            kafka_messages.pop(0)
            
thread = threading.Thread(target=consume_messages)
thread.daemon = True
thread.start()

class WeatherData(Resource):
    def get(self):
        records = list(collection.find())
        for record in records:
            record['_id'] = str(record['_id'])
        return jsonify(records)

class LiveWeatherUpdates(Resource):
    def get(self):
        return jsonify(kafka_messages) 
    
api.add_resource(WeatherData, '/api/weather_data')
api.add_resource(LiveWeatherUpdates, '/api/live_updates')

if __name__ == '__main__':
    app.run(debug=True)