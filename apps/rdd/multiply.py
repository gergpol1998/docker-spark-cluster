from pyspark.sql import SparkSession

# สร้าง SparkSession เพื่อเริ่มต้นใช้งาน PySpark
spark = SparkSession.builder \
    .appName("Distributed Data Processing with PySpark") \
    .getOrCreate()

# สร้าง RDD จากข้อมูลในรูปแบบ List
data_list = [1, 2, 3, 4, 5]
rdd = spark.sparkContext.parallelize(data_list)

# ใช้ Transformation เพื่อทำการ map และคูณค่าข้อมูลทุกตัวด้วย 2
rdd_transformed = rdd.map(lambda x: x * 2)

# ใช้ Action เพื่อแสดงผลลัพธ์
print("Original Data:")
print(rdd.collect())  # แสดงผลลัพธ์ข้อมูลที่เป็น List

print("Transformed Data (multiplied by 2):")
print(rdd_transformed.collect())  # แสดงผลลัพธ์ข้อมูลหลังจากถูกคูณด้วย 2

# ปิด SparkSession เมื่อสิ้นสุดการใช้งาน
spark.stop()
