from kafka import KafkaProducer

def kafka_producer_helloworld():
    # Replace 'your_bootstrap_servers' with the Kafka brokers' addresses, e.g., 'localhost:9092'
    bootstrap_servers = '172.25.0.12:9092'
    topic = 'hello_world'

    # Kafka producer configuration
    producer_config = {
        'bootstrap_servers': bootstrap_servers
    }

    # Create a Kafka producer instance
    producer = KafkaProducer(**producer_config)

    # Produce "Hello, World!" message to Kafka topic
    message = "Hello, World!"
    producer.send(topic, value=message.encode('utf-8'))

    # Wait for the message to be sent (optional)
    producer.flush()

    # Close the producer to release resources
    producer.close()

if __name__ == '__main__':
    kafka_producer_helloworld()
