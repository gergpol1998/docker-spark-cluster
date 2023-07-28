# Imports
from pyspark.sql import SparkSession

# Create SparkSession
spark = SparkSession.builder \
           .appName('SparkByExamples2.com') \
           .config("spark.jars", "/opt/spark-apps/test_db/postgresql-42.6.0.jar") \
           .getOrCreate()

# Read CSV file into a Spark DataFrame
csv_file_path = "/opt/spark-data/1500000 BT Records.csv"
spark_df = spark.read.csv(csv_file_path, header=True, inferSchema=True)

# Write to SQL Table
spark_df.write \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://172.25.0.17:5432/test") \
    .option("driver", "org.postgresql.Driver") \
    .option("dbtable", "bank") \
    .option("user", "root") \
    .option("password", "root") \
    .save()

spark_df.show()
