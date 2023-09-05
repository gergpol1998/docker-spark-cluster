from pyspark.sql import SparkSession
from dotenv import load_dotenv
import os
from pyspark.sql.functions import split, expr, col, current_timestamp, to_timestamp
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
    StructField("system_date", StringType()),
    StructField("product_class", StringType()),
    StructField("product_group", StringType()),
    StructField("product_type", StringType()),
    StructField("account_number", StringType()),
    StructField("account_re_opened_flag", StringType()),
    StructField("old_credit_limit", StringType()),
    StructField("new_credit_limit", StringType()),
    StructField("transaction_time", StringType()),
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

# Convert system_date to date
df = df.withColumn("system_date", col("system_date").cast("date"))

# Convert transaction_time to time
df = df.withColumn("transaction_time", to_timestamp("transaction_time", "HH:mm:ss"))

# Convert account_number to int
df = df.withColumn("account_number", col("account_number").cast("int"))

# Convert old_credit_limit to int
df = df.withColumn("old_credit_limit", col("old_credit_limit").cast("int"))

# Convert new_credit_limit to int
df = df.withColumn("new_credit_limit", col("new_credit_limit").cast("int"))

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
                                   col("system_date").isNotNull() &
                                   col("product_class").isNotNull() &
                                   col("product_group").isNotNull() &
                                   col("product_type").isNotNull() &
                                   col("account_number").isNotNull() &
                                   col("account_re_opened_flag").isNotNull() &
                                   col("old_credit_limit").isNotNull() &
                                   col("new_credit_limit").isNotNull() &
                                   col("transaction_time").isNotNull())

        # Add timestamp columns
        valid_df_with_timestamps = valid_df.withColumn("created_at", current_timestamp()) \
                                          .withColumn("updated_at", current_timestamp())
        # Write data to PostgreSQL
        valid_df_with_timestamps.write.jdbc(url=jdbc_url, table="accounts", mode="append", properties=properties)
    except Exception as e:
        print("An error occurred:", e)

# Write data to PostgreSQL
query = df.writeStream \
    .outputMode("append") \
    .foreachBatch(write_to_postgres) \
    .start()

# Await termination
query.awaitTermination()

