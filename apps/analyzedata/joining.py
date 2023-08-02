from pyspark.sql import SparkSession

# สร้าง SparkSession
spark = SparkSession.builder.appName("Joining Example").getOrCreate()

# อ่านข้อมูลจากไฟล์ CSV เก็บใน DataFrame df1 และ df2
df1 = spark.read.csv("/opt/spark-data/sales_data.csv", header=True, inferSchema=True)
df2 = spark.read.csv("/opt/spark-data/customer_data.csv", header=True, inferSchema=True)

# แสดงข้อมูลใน DataFrame df1 และ df2
print("DataFrame df1:")
df1.show()

print("DataFrame df2:")
df2.show()

# ใช้ join() เพื่อรวมข้อมูลจาก DataFrame df1 และ df2 ด้วยคอลัมน์ "CustomerID" (inner join)
joined_df = df1.join(df2, on="CustomerID", how="inner")

# แสดงข้อมูลใน DataFrame หลังจากที่รวมข้อมูลเชิงลึกแล้ว
print("Joined DataFrame:")
joined_df.show()

# ปิด SparkSession
spark.stop()
