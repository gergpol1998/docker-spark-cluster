from pyspark.sql import SparkSession
from dotenv import load_dotenv
import os
from pyspark.sql.functions import split, expr, col, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, TimestampType

# Initialize Spark
spark = SparkSession.builder \
    .appName("KafkaConsumer") \
    .getOrCreate()

# Load configuration from .env file
load_dotenv()

# Kafka configuration
bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
topic = os.getenv("KAFKA_TOPIC")

# Read data from Kafka topic
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", bootstrap_servers) \
    .option("subscribe", topic) \
    .option("startingOffsets", "latest") \
    .load()

# Convert value to string
df = df.withColumn("value", expr("CAST(value AS STRING)"))

# Define customer data structure
customer_fields = [
    StructField("customer_id", StringType()),
    StructField("thai_title_name", StringType()),
    StructField("thai_first_name", StringType()),
    StructField("thai_last_name", StringType()),
    StructField("eng_title_name", StringType()),
    StructField("eng_first_name", StringType()),
    StructField("eng_last_name", StringType()),
    StructField("date_of_birth", StringType()),
    StructField("sex", StringType()),
    StructField("nationality", StringType()),
    StructField("created_at", TimestampType()), 
    StructField("updated_at", TimestampType())
]

# Define customer schema
customer_schema = StructType(customer_fields)

# Split the value by '|' and create a new column for each field
split_cols = split(df["value"], "\\|")
for i, col_name in enumerate(customer_schema.fieldNames()):
    df = df.withColumn(col_name, split_cols.getItem(i))

# Select only the desired fields
selected_fields = customer_schema.fieldNames()
df = df.select(selected_fields)

# Convert customer_id to bigint
df = df.withColumn("customer_id", col("customer_id").cast("bigint"))

# Convert DateOfBirth to date
df = df.withColumn("date_of_birth", col("date_of_birth").cast("date"))

# JDBC URL for PostgreSQL
jdbc_url = "jdbc:postgresql://172.25.0.18:5432/postgres"
properties = {
    "user": "postgres",
    "password": "postgres",
    "driver": "org.postgresql.Driver"
}

# Write data to PostgreSQL with validation and try-except
def write_to_postgres(batch_df, batch_id):
    try:
        # Validate data
        valid_df = batch_df.filter(col("customer_id").isNotNull() &
                                   col("thai_title_name").isNotNull() &
                                   col("thai_first_name").isNotNull() &
                                   col("thai_last_name").isNotNull() &
                                   col("eng_title_name").isNotNull() &
                                   col("eng_first_name").isNotNull() &
                                   col("eng_last_name").isNotNull() &
                                   col("date_of_birth").isNotNull() &
                                   col("sex").isNotNull() &
                                   col("nationality").isNotNull())
        # Add timestamp columns
        valid_df_with_timestamps = valid_df.withColumn("created_at", current_timestamp()) \
                                          .withColumn("updated_at", current_timestamp())
        # Write data to PostgreSQL
        valid_df_with_timestamps.write.jdbc(url=jdbc_url, table="customers", mode="append", properties=properties)
    except Exception as e:
        print("An error occurred:", e)

# Write data to PostgreSQL
query = df.writeStream \
    .outputMode("append") \
    .foreachBatch(write_to_postgres) \
    .start()

# Await termination
query.awaitTermination()

