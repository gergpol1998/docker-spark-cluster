from pyspark.sql import SparkSession

# สร้าง SparkSession
spark = SparkSession.builder.appName("Read and Write Data CSV").getOrCreate()

# อ่านข้อมูลจากไฟล์ CSV
df_csv = spark.read.csv("/opt/spark-data/Advertising.csv", header=True, inferSchema=True)

# แสดงโครงสร้างของ DataFrame ที่อ่านเข้ามาจาก CSV
df_csv.printSchema()

# แสดงข้อมูลใน DataFrame ที่อ่านเข้ามาจาก CSV
df_csv.show()

# เขียนข้อมูลลงในไฟล์ CSV
df_csv.write.csv("/opt/spark-data/output/output.csv",header=True, mode="overwrite")


# ปิด SparkSession
spark.stop()