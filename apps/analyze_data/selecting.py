from pyspark.sql import SparkSession

# สร้าง SparkSession
spark = SparkSession.builder.appName("Selecting Example").getOrCreate()

# อ่านข้อมูลจากไฟล์ CSV
df = spark.read.csv("/opt/spark-data/sales_data.csv", header=True, inferSchema=True)

# แสดงข้อมูลใน DataFrame ก่อนที่เราจะเลือกคอลัมน์
print("Original DataFrame:")
df.show()

# ใช้ select() เพื่อเลือกคอลัมน์ที่ต้องการใน DataFrame
selected_df = df.select("Product", "Category", "Sales")

# แสดงข้อมูลใน DataFrame หลังจากที่เลือกคอลัมน์
print("DataFrame after Selecting Columns:")
selected_df.show()

# ปิด SparkSession
spark.stop()
