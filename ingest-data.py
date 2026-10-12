import json 
import time 
import requests
import os
from kafka import KafkaProducer

INTERVAL = 10
API_KEY = os.environ["JCDECAUX_API_KEY"]
TOPIC_NAME = "velib-stations"
API_URL = url = "https://api.jcdecaux.com/vls/v1/stations?apiKey={}".format(API_KEY)


producer = KafkaProducer(bootstrap_servers="localhost:9092",
                         value_serializer=str.encode)

while True: 
    stations = requests.get(API_URL).json()
    for station_log in stations: 
        producer.send(TOPIC_NAME, json.dumps(station_log))
    producer.flush()
    time.sleep(INTERVAL)
