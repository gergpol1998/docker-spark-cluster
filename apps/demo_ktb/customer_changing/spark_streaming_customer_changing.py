from pyspark.sql import SparkSession
from dotenv import load_dotenv
import os
from pyspark.sql.functions import split, expr, col, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, TimestampType

# Initialize Spark
spark = SparkSession.builder \
    .appName("Customer_Changing_insert & update") \
    .getOrCreate()

# Load config from .env file
load_dotenv()

# Kafka config
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

# Define customer_changing data structure
customer_changing_fields = [
    StructField("customer_id", StringType()),
    StructField("trans_datetime", StringType()),
    StructField("product_class", StringType()),
    StructField("product_group", StringType()),
    StructField("product_type", StringType()),
    StructField("account_number", StringType()),
    StructField("trans_sequence", StringType()),
    StructField("created_at", TimestampType()), 
    StructField("updated_at", TimestampType())
]

# Define customer_changing schema
customer_changing_schema = StructType(customer_changing_fields)

# Split the value by '|' and create a new column for each field
split_cols = split(df["value"], "\\|")
for i, col_name in enumerate(customer_changing_schema.fieldNames()):
    df = df.withColumn(col_name, split_cols.getItem(i))

# Select only the desired fields
selected_fields = customer_changing_schema.fieldNames()
df = df.select(selected_fields)

# Convert customer_id to bigint
df = df.withColumn("customer_id", col("customer_id").cast("bigint"))

# Convert trans_datetime to datetime
df = df.withColumn("trans_datetime", expr("to_timestamp(trans_datetime, 'yyyy-MM-dd HH:mm:ss')"))

# Convert account_number to int
df = df.withColumn("account_number", col("account_number").cast("int"))

# Convert trans_sequence to int
df = df.withColumn("trans_sequence", col("trans_sequence").cast("int"))

# JDBC URL for PostgreSQL
jdbc_url = "jdbc:postgresql://172.25.0.18:5432/postgres"
properties = {
    "user": "postgres",
    "password": "postgres",
    "driver": "org.postgresql.Driver"
}

# Define a function to write transformed data to the customer_changings table
def write_to_customer_changings(batch_df, batch_id):
    try:
        # Validate data and add timestamp columns
        valid_df = batch_df.filter(
            col("customer_id").isNotNull() &
            col("trans_datetime").isNotNull() &
            col("product_class").isNotNull() &
            col("product_group").isNotNull() &
            col("product_type").isNotNull() &
            col("account_number").isNotNull() &
            col("trans_sequence").isNotNull()
        ).withColumn("created_at", current_timestamp()) \
         .withColumn("updated_at", current_timestamp())

        # Write data to PostgreSQL customer_changings table
        valid_df.write.jdbc(url=jdbc_url, table="customer_changings", mode="append", properties=properties)
        

    except Exception as e:
        print("An error occurred:", e)



# Write data to PostgreSQL
query = df.writeStream \
    .outputMode("append") \
    .foreachBatch(write_to_customer_changings) \
    .start()

# Await termination
query.awaitTermination()
