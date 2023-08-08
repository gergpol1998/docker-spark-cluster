from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# สร้าง SparkSession
spark = SparkSession.builder.appName("Aggregation Example").getOrCreate()

# อ่านข้อมูลจากไฟล์ CSV
df = spark.read.csv("/opt/spark-data/sales_data.csv", header=True, inferSchema=True)

# แสดงข้อมูลใน DataFrame
print("Original DataFrame:")
df.show()

# ใช้ groupBy() เพื่อกลุ่มและจับคู่ค่าที่ต้องการ
grouped_df = df.groupBy("Category")

# ใช้ฟังก์ชันทางสถิติเช่น sum(), avg(), min(), max() เพื่อคำนวณค่าสถิติในแต่ละกลุ่ม
sum_df = grouped_df.agg({"Sales": "sum"})
avg_df = grouped_df.agg({"Profit": "avg"})
min_df = grouped_df.agg({"Quantity": "min"})
max_df = grouped_df.agg({"Discount": "max"})

# แสดงข้อมูลใน DataFrame ที่คำนวณค่าสถิติแล้ว
print("Sum of Sales by Category:")
sum_df.show()

print("Average Profit by Category:")
avg_df.show()

print("Minimum Quantity by Category:")
min_df.show()

print("Maximum Discount by Category:")
max_df.show()

# ปิด SparkSession
spark.stop()
