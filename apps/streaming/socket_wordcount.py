from pyspark.sql import SparkSession
from pyspark.sql.functions import *


if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("Streaming Word Count") \
        .master("local[4]")\
        .config("spark.streaming.stopGracefullyOnShutdown", "true") \
        .config("spark.sql.shuffle.partitions", 3) \
        .getOrCreate()

    lines_df = spark.readStream \
        .format("socket") \
        .option("host", "192.168.1.40") \
        .option("port", "9999") \
        .load()

    # lines_df.printSchema()

    # words_df = lines_df.select(explode(split("value", " ")).alias("word"))
    words_df = lines_df.select(expr("explode(split(value,' ')) as word"))
    counts_df = words_df.groupBy("word").count()

    word_count_query = counts_df.writeStream \
        .format("console") \
        .outputMode("complete") \
        .start()
    
    word_count_query.awaitTermination()
