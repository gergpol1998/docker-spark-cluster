from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json
from pyspark.sql.types import StructType, StructField, StringType, LongType

# Create SparkSession
spark = SparkSession.builder \
    .appName('SparkByExamples.com') \
    .config("spark.jars", "/opt/spark-apps/test_db/postgresql-42.6.0.jar") \
    .getOrCreate()

# Read data from a Kafka topic and only select the "value" column
input_topic = "test"
kafka_server = "172.25.0.12:9092"

kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_server) \
    .option("subscribe", input_topic) \
    .load()

# Convert the "value" column to a String and define the schema for the DataFrame
schema = StructType([
    StructField("id", LongType(), True),
    StructField("name", StringType(), True),
    StructField("age", LongType(), True),
    StructField("gender", StringType(), True)
])

parsed_df = kafka_df.selectExpr("CAST(value AS STRING)") \
    .select(from_json("value", schema).alias("data")) \
    .select("data.*")

# Write the processed data to PostgreSQL in real-time
output_table = "employee"
output_url = "jdbc:postgresql://172.25.0.17:5432/test"
output_properties = {
    "user": "root",
    "password": "root",
    "driver": "org.postgresql.Driver"
}

query = parsed_df.writeStream \
    .foreachBatch(lambda df, batchId: df.write.jdbc(url=output_url, table=output_table, mode="append", properties=output_properties)) \
    .start()

query.awaitTermination()
