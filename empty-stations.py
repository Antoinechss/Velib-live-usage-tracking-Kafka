import json
from kafka import KafkaProducer, KafkaConsumer

consumer = KafkaConsumer("stations-status", bootstrap_servers="localhost:9092", value_deserializer=json.loads)
producer = KafkaProducer(bootstrap_servers="localhost:9092", value_serializer=str.encode)

status_history = {}

for message in consumer:
    current = message.value
    station_key = (current["contract_name"], current["number"])
    previous = status_history.get(station_key)

    if previous is not None:

        if current["available_bikes"] == 0 and previous["available_bikes"] != 0: 
            print("Station {} located in {} is empty".format(current["number"], current["contract_name"]))
            producer.send("empty-stations", json.dumps(current))
        if current["available_bikes"] != 0 and previous["available_bikes"] == 0: 
            print("Station {} located in {} is no longer empty".format(current["number"], current["contract_name"]))
            producer.send("empty-stations", json.dumps(current))

    status_history[station_key] = current


