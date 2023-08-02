from pyspark.sql import SparkSession

# สร้าง SparkSession
spark = SparkSession.builder.appName("Read and Write Data json").getOrCreate()

# อ่านข้อมูลจากไฟล์ JSON
df_json = spark.read.json("/opt/spark-data/iris.json")

# แสดงโครงสร้างของ DataFrame ที่อ่านเข้ามาจาก JSON
df_json.printSchema()

# แสดงข้อมูลใน DataFrame ที่อ่านเข้ามาจาก JSON
df_json.show()

# เขียนข้อมูลลงในไฟล์ JSON
df_json.write.json("/opt/spark-data/output/output.json", mode="overwrite")

# ปิด SparkSession
spark.stop()