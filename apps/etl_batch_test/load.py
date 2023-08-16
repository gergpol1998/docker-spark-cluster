from pyspark.sql import DataFrame

def load(type: str, df: DataFrame, target: str):

    if type == "CSV":
    # Write data on filesystem
       df.write.format("CSV").mode("overwrite").options(header=True).save(target)
       print(f"Data succesfully loaded to filesystem !!")