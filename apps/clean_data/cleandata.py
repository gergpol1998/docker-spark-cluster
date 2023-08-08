from pyspark.sql import SparkSession
from pyspark.ml.feature import Imputer

# สร้าง SparkSession
spark = SparkSession.builder.appName("Data Cleaning Example").getOrCreate()

# อ่านข้อมูลจากไฟล์ CSV
df = spark.read.csv("/opt/spark-data/1500000 BT Records.csv", header=True, inferSchema=True)

# แสดงข้อมูลใน DataFrame ก่อนทำความสะอาด
print("Before Data Cleaning:")
df.show()

# กรองแถวที่คอลัมน์ 'Deposits' ไม่เท่ากับ '00.00'
df_cleaned = df.filter(df["Deposits"] != "00.00")

# ลบคอลัมน์ 'Withdrawls' ออกจาก DataFrame
df_cleaned = df_cleaned.drop("Withdrawls")

# แสดงข้อมูลใน DataFrame หลังจากทำความสะอาดและแสดงคอลัมน์ 'Withdrawls'
print("After Data Cleaning:")
df_cleaned.show()

# ปิด SparkSession
spark.stop()
