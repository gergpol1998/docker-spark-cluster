from kafka import KafkaProducer
from dotenv import load_dotenv
import os
import random
from datetime import datetime, timedelta
import time

# Load configuration from .env file
load_dotenv()

# Kafka configuration
bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
topic = os.getenv("KAFKA_TOPIC")

# Generate customer data
def generate_customer():
    titles = ["Mr.", "Mrs.", "Miss"]
    first_names = ["John", "Jane", "Robert", "Emily"]
    last_names = ["Smith", "Johnson", "Williams", "Brown"]
    nationalities = ["Thai", "American", "British", "Chinese"]

    customer = {
        "customer_id": str(random.randint(1000000, 9999999)),
        "ThaiTitleName": random.choice(titles),
        "ThaiFirstName": random.choice(first_names),
        "ThaiLastName": random.choice(last_names),
        "EngTitleName": random.choice(titles),
        "EngFirstName": random.choice(first_names),
        "EngLastName": random.choice(last_names),
        "DateOfBirth": (datetime.now() - timedelta(days=random.randint(365 * 20, 365 * 60))).strftime("%Y-%m-%d"),
        "Sex": random.choice(["M", "F"]),
        "nationality": random.choice(nationalities)
    }

    return customer

# Create Kafka producer
producer = KafkaProducer(bootstrap_servers=bootstrap_servers, value_serializer=lambda v: v.encode('utf-8'))

# Produce messages
for _ in range(100):  # Produce 10 messages
    customer = generate_customer()
    message = "|".join(customer.values())  # Join values with '|'
    producer.send(topic, value=message)
    time.sleep(5)
    print(message)

# Close the producer
producer.close()
