from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder \
    .appName("KafkaSparkStructuredStreamingExample") \
    .config("spark.streaming.stopGracefullyOnShutdown","false") \
    .getOrCreate()

# Define Kafka parameters
kafka_params = {
    "kafka.bootstrap.servers": "172.25.0.12:9092",  # Replace with your Kafka brokers' addresses
    "subscribe": "hello_world",  # Kafka topic to subscribe to
    "startingOffsets": "earliest"  # Start from the earliest offset
}

# Read data from Kafka using structured streaming
kafka_stream = spark.readStream \
    .format("kafka") \
    .options(**kafka_params) \
    .load()

# Extract the value column (Kafka message)
kafka_stream = kafka_stream.selectExpr("CAST(value AS STRING)")

# Start the query to process the Kafka stream and show the result in the console
query = kafka_stream.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

# Wait for the query to terminate
query.awaitTermination()

# Stop the SparkSession
spark.stop()
