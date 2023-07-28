from kafka import KafkaProducer
import json

def kafka_producer_example():
    # Replace 'your_bootstrap_servers' with the Kafka brokers' addresses, e.g., 'localhost:9092'
    bootstrap_servers = '172.25.0.12:9092'
    topic = 'test'

    # Kafka producer configuration
    producer_config = {
        'bootstrap_servers': bootstrap_servers,
        'value_serializer': lambda v: json.dumps(v).encode('utf-8')
    }

    # Create a Kafka producer instance
    producer = KafkaProducer(**producer_config)

    # Produce some sample data to Kafka
    for i in range(10):
        data = {
            'id': i,
            'message': f'Message {i}'
        }

        # Send the data to the Kafka topic
        producer.send(topic, value=data)

    # Wait for all messages to be sent (optional)
    producer.flush()

    # Close the producer to release resources
    producer.close()

if __name__ == '__main__':
    kafka_producer_example()
