from pyspark.sql import SparkSession

def extract(spark: SparkSession, type: str, source: str):

    #Read data from postgres database
    if type == "JDBC":
        output_df = spark.read \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://172.25.0.17:5432/test") \
        .option("driver", "org.postgresql.Driver") \
        .option("dbtable",source) \
        .option("user", "root") \
        .option("password", "root") \
        .load()
        return output_df
    
    if type == "CSV":
        #Read data from filesystem
        output_df = spark.read.format('CSV') \
        .options(header=True,inferSchema=True) \
        .load(source)
        return output_df