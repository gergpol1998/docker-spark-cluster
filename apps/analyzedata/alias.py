from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# สร้าง SparkSession
spark = SparkSession.builder.appName("Column Alias Example").getOrCreate()

# อ่านข้อมูลจากไฟล์ CSV
df = spark.read.csv("/opt/spark-data/sales_data.csv", header=True, inferSchema=True)

# แสดงข้อมูลใน DataFrame ก่อนที่เราจะกำหนดชื่อใหม่ให้กับคอลัมน์
print("Original DataFrame:")
df.show()

# ใช้ alias() เพื่อกำหนดชื่อใหม่ให้กับคอลัมน์ "Sales" เป็น "TotalSales"
df_with_alias = df.select(df["Sales"].alias("TotalSales"))

# แสดงข้อมูลใน DataFrame หลังจากที่กำหนดชื่อใหม่ให้กับคอลัมน์ "Sales"
print("DataFrame with Column Alias:")
df_with_alias.show()

# ปิด SparkSession
spark.stop()
