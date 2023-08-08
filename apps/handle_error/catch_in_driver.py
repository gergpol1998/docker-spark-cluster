from pyspark.sql import SparkSession

# สร้าง SparkSession
spark = SparkSession.builder.appName("catch_in_driver").getOrCreate()

try:
    # ตัวอย่าง: อ่านข้อมูลจากไฟล์ CSV และนับจำนวนแถว
    df = spark.read.csv("/opt/spark-data/sales.csv")
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
    # กรณีเกิด exception ใน driver program
    print("An exception occurred:", str(e))

# ปิด SparkSession
spark.stop()
