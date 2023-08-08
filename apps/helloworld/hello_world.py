from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Spark Hello World") \
    .master("local[4]")\
    .getOrCreate()

data = [("Hello, World!",)]
df = spark.createDataFrame(data, ["message"])

df.show()
spark.stop()
