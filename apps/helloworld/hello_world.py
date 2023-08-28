from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Spark Hello World") \
    .getOrCreate()

data = [("Hello, World!",)]
df = spark.createDataFrame(data, ["message"])

df.show()
spark.stop()
