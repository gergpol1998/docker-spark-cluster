from pyspark.sql import SparkSession


def spark_inst():
    return SparkSession.builder.master('spark://spark-master-5688d7ff8d-4rzsf:7077') \
    .appName('etl_batch_test')  \
    .getOrCreate()
