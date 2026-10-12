from kafka import KafkaConsumer, KafkaProducer
import json

consumer = KafkaConsumer("velib-stations", bootstrap_servers="localhost:9092", value_deserializer=json.loads)
producer = KafkaProducer(bootstrap_servers="localhost:9092", value_serializer=str.encode)

status_history = {}

for message in consumer:
    current = message.value
    station_key = (current["contract_name"], current["number"])
    previous = status_history.get(station_key)

    if previous is not None:

        available_bikes_diff = (previous["available_bikes"] != current["available_bikes"])
        free_slots_diff = (previous["available_bike_stands"] != current["available_bike_stands"])

        if available_bikes_diff or free_slots_diff:
            print("Station change in status {} ({})".format(current["number"], current["contract_name"]))
            producer.send("stations-status", json.dumps(current))

    status_history[station_key] = current
