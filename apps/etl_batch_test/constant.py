from pyspark.sql import SparkSession


def spark_inst():
    return SparkSession.builder.master('spark://d5ff24970388:7077') \
    .appName('etl_batch_test')  \
    .getOrCreate()
