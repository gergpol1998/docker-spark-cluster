from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# สร้าง SparkSession
spark = SparkSession.builder.appName("Calculating New Column").getOrCreate()

# อ่านข้อมูลจากไฟล์ CSV
df = spark.read.csv("/opt/spark-data/sales_data.csv", header=True, inferSchema=True)

# แสดงข้อมูลใน DataFrame ก่อนที่เราจะคำนวณคอลัมน์ใหม่
print("Original DataFrame:")
df.show()

# ใช้ withColumn() เพื่อคำนวณคอลัมน์ใหม่ ในตัวอย่างนี้คือคอลัมน์ "TotalProfit" 
# ซึ่งคำนวณค่าจากคอลัมน์ "Sales" และ "Profit" โดยใช้ฟังก์ชัน sum() ของ PySpark
df_with_new_column = df.withColumn("TotalProfit", col("Sales") + col("Profit"))

# แสดงข้อมูลใน DataFrame หลังจากที่คำนวณคอลัมน์ใหม่แล้ว
print("DataFrame with New Column:")
df_with_new_column.show()

# ปิด SparkSession
spark.stop()
