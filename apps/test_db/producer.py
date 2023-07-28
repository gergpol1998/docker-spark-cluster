from kafka import KafkaProducer
import json
import time

# Replace with your Kafka broker address and port
kafka_broker = "172.25.0.12:9092"
topic = "test"

producer = KafkaProducer(bootstrap_servers=kafka_broker,
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))

# Simulate data production
data_records = [
    {"id":1,"name":"Alice","age":30,"gender":"F"},
    {"id":2,"name": "Bob", "age": 25,"gender":"M"},
    {"id":3,"name": "Carol", "age": 35,"gender":"M"},
]

for data in data_records:
    producer.send(topic, value=data)
    time.sleep(1)  # To simulate real-time data production
    print (data)

producer.close()
