from pyspark.sql import DataFrame

def load(type: str, df: DataFrame, target: str):
    #Load the data based on type
    '''
    :param type: Input Storage type (JDBC|CSV) Based on type data stored in MySQL or FileSystem
    :param df: Input Dataframe
    :param target: Input target 
             -For filesystem - Location where to store the data
             -For MySQL - table name
    '''
    # Write data on postgres database with table name
    if type == "JDBC":
        df.write \
        .format("jdbc") \
        .mode("overwrite") \
        .option("url", "jdbc:postgresql://172.25.0.17:5432/test") \
        .option("driver", "org.postgresql.Driver") \
        .option("dbtable",target) \
        .option("user", "root") \
        .option("password", "root") \
        .save()
        print(f'Data succesfully loaded to Postgres Database !!')

    if type == "CSV":
    # Write data on filesystem
       df.write.format("CSV").mode("overwrite").options(header=True).save(target)
       print(f"Data succesfully loaded to filesystem !!")