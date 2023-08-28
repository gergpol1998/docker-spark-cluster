from kafka import KafkaProducer
from dotenv import load_dotenv
import os
import random
import time

# Load configuration from .env file
load_dotenv()

# Kafka configuration
bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
topic = os.getenv("KAFKA_TOPIC")

# Generate new account data
def generate_new_account():
    account = {
        "customer_id": str(random.randint(1000000, 9999999)),
        "SystemDate": time.strftime("%Y-%m-%d"),
        "ProductClass": "Class X",
        "ProductGroup": "Group Y",
        "ProductType": "Type Z",
        "AccountNumber": str(random.randint(1000000, 9999999)),
        "AccountRe_openedFlag": random.choice(["Y", "N"]),
        "OldCreditLimit": round(random.uniform(5000, 20000), 2),
        "NewCreditLimit": round(random.uniform(5000, 30000), 2),
        "TransactionTime": time.strftime("%H:%M:%S"),
    }
    # Convert float values to strings
    account["OldCreditLimit"] = str(account["OldCreditLimit"])
    account["NewCreditLimit"] = str(account["NewCreditLimit"])

    return account

# Create Kafka producer
producer = KafkaProducer(bootstrap_servers=bootstrap_servers, value_serializer=lambda v: v.encode('utf-8'))

# Produce messages
for _ in range(100):  # Produce 10 messages
    new_account = generate_new_account()
    message = "|".join(new_account.values())  # Join values with '|'
    producer.send(topic, value=message)
    time.sleep(5)
    print(message)

# Close the producer
producer.close()
