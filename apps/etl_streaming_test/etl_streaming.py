from pyspark.sql import SparkSession

def etl_streaming_example():
    spark = SparkSession.builder.master('local[4]') \
        .appName('etl_streaming_test') \
        .getOrCreate()