from pyspark.sql import SparkSession


def spark_inst():
    return SparkSession.builder.master('http://18.140.117.103:30080/') \
    .appName('etl_batch_test')  \
    .getOrCreate()
