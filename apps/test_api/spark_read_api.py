import requests
from pyspark.sql import SparkSession
import json
from pyspark.sql import *

url = "https://jsonplaceholder.typicode.com/todos"
response = requests.get(url)

# Check the status code to ensure the request was successful
if response.status_code == 200:
    data = response.json()
else:
    print("Failed to fetch data")

# Create SparkSession
spark = SparkSession.builder \
    .appName("Fetch API Data") \
    .getOrCreate()

# Convert the data to JSON
json_data = json.dumps(data)

# Load the data into a DataFrame
df = spark.read.json(spark.sparkContext.parallelize([json_data]))

# Select Specific
df2 = df.select("id", "title", "completed")

df2.show()