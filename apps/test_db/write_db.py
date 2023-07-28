# Imports
from pyspark.sql import SparkSession

# Create SparkSession
spark = SparkSession.builder \
           .appName('SparkByExamples.com') \
           .config("spark.jars", "/opt/spark-apps/test_db/postgresql-42.6.0.jar") \
           .getOrCreate()

# Create DataFrame 
columns = ["id", "name", "age", "gender"]
data = [(1, "James", 30, "M"), (2, "Ann", 40, "F"),
        (3, "Jeff", 41, "M"), (4, "Jennifer", 20, "F")]

sampleDF = spark.sparkContext.parallelize(data).toDF(columns)

# Write to SQL Table
sampleDF.select("id", "name", "age", "gender").write \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://172.25.0.17:5432/test") \
    .option("driver", "org.postgresql.Driver") \
    .option("dbtable", "employee") \
    .option("user", "root") \
    .option("password", "root") \
    .save()

sampleDF.show()
