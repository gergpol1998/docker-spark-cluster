from kafka import KafkaProducer
import csv

def kafka_producer():

    # Replace 'your_bootstrap_servers' with the Kafka brokers' addresses, e.g., 'localhost:9092'
    bootstrap_servers = "172.25.0.12:9092,172.25.0.13:9092"
    # Create a Kafka producer instance
    producer = KafkaProducer(
        bootstrap_servers = bootstrap_servers,
        value_serializer=lambda v: str(v).encode('utf-8')
    )

    # Send messages to the Kafka topic
    topic = 'topic_name'
    csv_file_path = 'path_to_your_csv_file.csv'

    # Read data from the CSV file and send as messages
    with open(csv_file_path, 'r') as csv_file:
        csv_reader = csv.DictReader(csv_file)
        for row in csv_reader:
            # Send each row (message) to Kafka
            producer.send(topic, value=row)

    # Close the producer
    producer.close()

