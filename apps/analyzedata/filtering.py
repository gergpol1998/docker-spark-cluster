from pyspark.sql import SparkSession

# สร้าง SparkSession
spark = SparkSession.builder.appName("Filtering Example").getOrCreate()

# อ่านข้อมูลจากไฟล์ CSV
df = spark.read.csv("/opt/spark-data/sales_data.csv", header=True, inferSchema=True)

# แสดงข้อมูลใน DataFrame ก่อนที่จะกรองแถว
print("Original DataFrame:")
df.show()

# ใช้ filter() เพื่อกรองแถวตามเงื่อนไขที่ต้องการ
filtered_df = df.filter(df["Sales"] > 1000)  # กรองแถวที่มีค่า Sales มากกว่า 1000

# แสดงข้อมูลใน DataFrame หลังจากที่กรองแถว
print("DataFrame after Filtering:")
filtered_df.show()

# ปิด SparkSession
spark.stop()
