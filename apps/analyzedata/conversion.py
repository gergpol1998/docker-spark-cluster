from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# สร้าง SparkSession
spark = SparkSession.builder.appName("Data Type Conversion Example").getOrCreate()

# อ่านข้อมูลจากไฟล์ CSV
df = spark.read.csv("/opt/spark-data/sales_data.csv", header=True, inferSchema=True)

# แสดงข้อมูลใน DataFrame ก่อนที่เราจะแปลงประเภทข้อมูล
print("Original DataFrame:")
df.show()

# ใช้ cast() เพื่อแปลงประเภทข้อมูลในคอลัมน์ "Sales" จาก StringType เป็น IntegerType
df_with_integer_sales = df.withColumn("Sales", col("Sales").cast("int"))

# แสดงข้อมูลใน DataFrame หลังจากที่แปลงประเภทข้อมูลคอลัมน์ "Sales" เป็น IntegerType
print("DataFrame after Data Type Conversion:")
df_with_integer_sales.show()

# ปิด SparkSession
spark.stop()
