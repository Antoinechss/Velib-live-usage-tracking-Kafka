from datetime import datetime
from kafka import KafkaConsumer

consumer = KafkaConsumer("velib-stations", 
                         "stations-status", 
                         "empty-stations",
                         bootstrap_servers="localhost:9092")

for message in consumer:
    timestamp = datetime.fromtimestamp(message.timestamp / 1000)
    print("{}, {}, {}, {}".format(message.topic, message.partition, message.offset, timestamp))
