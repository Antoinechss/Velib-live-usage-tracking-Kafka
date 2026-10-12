import json
from kafka import KafkaConsumer

consumer = KafkaConsumer("empty-stations", bootstrap_servers="localhost:9092", value_deserializer=json.loads)

for message in consumer:
    station = message.value
    if station['available_bikes'] == 0: 
        print("Station {} located at {} in city of {} is empty".format(station['number'],
                                                                       station['address'],
                                                                       station['contract_name']))