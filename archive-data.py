from kafka import KafkaConsumer 

consumer = KafkaConsumer("velib-stations", bootstrap_servers="localhost:9092")

with open("velib-stations-archive.txt", "a") as archive:
    for message in consumer:
        archive.write(message.value.decode() + "\n")
        archive.flush()
 