from kafka import KafkaProducer
from dotenv import load_dotenv
import os 
import random
from datetime import datetime

# Load config from .evn file
load_dotenv()

# Kafka config
bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
topic = os.getenv("KAFKA_TOPIC")

# Generate customer_changing data
def generate_customer_changing():
    customer_changing = {
        "customer_id": str(4468291),
        "TransDatetime": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "ProductClass": "Class A",
        "ProductGroup": "Group T",
        "ProductType": "Type J",
        "AccountNumber": str(random.randint(1000000, 9999999)),
        "TransSequence": str(random.randint(1000000, 9999999))
    }

    return customer_changing

# Create Kafka producer
producer = KafkaProducer(bootstrap_servers=bootstrap_servers, value_serializer=lambda v: v.encode('utf-8'))

# Produce messages
new_customer_changing = generate_customer_changing()
message = "|".join(new_customer_changing.values())  # Join values with '|'
producer.send(topic, value=message)
print(message)

# Close the producer
producer.close()