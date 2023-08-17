from pyspark.sql import SparkSession


def spark_inst():
    return SparkSession.builder.master('local[4]') \
    .appName('etl_batch_test')  \
    .getOrCreate()
