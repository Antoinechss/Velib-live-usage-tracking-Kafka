(.venv) (base) antoinechosson@antoine-mac Velib-live-usage-tracking-Kafka % cd /Users/antoinechosson/Velib-live-tracking-Kafka/Velib-live-usage-tracking-Kafka/kafka_2.13-4.3.1
bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic velib-stations
bin/kafka-topics.sh --list --bootstrap-server localhost:9092

Created topic velib-stations.
velib-stations
(.venv) (base) antoinechosson@antoine-mac kafka_2.13-4.3.1 % 

