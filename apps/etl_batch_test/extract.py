from pyspark.sql import SparkSession


def extract(spark: SparkSession, type: str, source: str):

    if type == "CSV":
        #Read data from filesystem
        output_df = spark.read.format('CSV') \
        .options(header=True,inferSchema=True) \
        .load(source)
        return output_df