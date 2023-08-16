# Imports
from pyspark.sql import SparkSession

try:
    # Create SparkSession
    spark = SparkSession.builder \
           .appName('write_log_to_postgresql') \
           .config("spark.jars", "/opt/spark-apps/test_db/postgresql-42.6.0.jar") \
           .getOrCreate()
    
    # ตัวอย่าง: อ่านข้อมูลจากไฟล์ CSV และนับจำนวนแถว
    df = spark.read.csv("/opt/spark-data/sales_data.csv")
    row_count = df.count()
    print("Number of rows:", row_count)
    
    # ตัวอย่าง: แบ่งข้อมูลเป็นสองส่วน (ถ้ามีข้อผิดพลาด)
    # เมื่อมีข้อผิดพลาดที่เกิดขึ้นในขั้นตอนการแบ่งข้อมูล เราจะต้องตั้งค่า partition_a และ partition_b เป็น None
    partition_a = None
    partition_b = None
    
    if row_count > 0:
        partition_a, partition_b = df.randomSplit([0.7, 0.3], seed=42)
    
    # ตัวอย่าง: กระทำข้อมูลใน partition_a
    if partition_a is not None:
        partition_a.show()
    
    # ตัวอย่าง: กระทำข้อมูลใน partition_b
    if partition_b is not None:
        partition_b.show()
    
except Exception as e:
    # Handle the exception and write error information to the database
    print("An error occurred:", str(e))
    
    # Create a DataFrame containing the error message
    error_message = str(e)
    error_df = spark.createDataFrame([(error_message,)], ["message"])
    
    # Write the error message to the "log_error" table in the database
    error_df.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://172.25.0.17:5432/test") \
        .mode("append") \
        .option("driver", "org.postgresql.Driver") \
        .option("dbtable", "log_error") \
        .option("user", "root") \
        .option("password", "root") \
        .save()
    
    spark.stop()