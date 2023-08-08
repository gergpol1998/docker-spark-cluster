from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# สร้าง SparkSession
spark = SparkSession.builder.appName("Ordering Example").getOrCreate()

# อ่านข้อมูลจากไฟล์ CSV
df = spark.read.csv("/opt/spark-data/sales_data.csv", header=True, inferSchema=True)

# แสดงข้อมูลใน DataFrame
print("Original DataFrame:")
df.show()

# ใช้ orderBy() เพื่อเรียงลำดับข้อมูลตามคอลัมน์ที่ต้องการ
# ในตัวอย่างนี้เราจะเรียงลำดับตามคอลัมน์ "Sales" ในลำดับน้อยไปมาก (ascending order)
sorted_df = df.orderBy("Sales")

# แสดงข้อมูลใน DataFrame หลังจากเรียงลำดับ
print("DataFrame after Sorting:")
sorted_df.show()

# ปิด SparkSession
spark.stop()
