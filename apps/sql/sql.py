from pyspark.sql import SparkSession

# สร้าง SparkSession
spark = SparkSession.builder \
    .appName("PySpark SQL Example") \
    .getOrCreate()

# โหลดข้อมูลจากไฟล์ CSV
df = spark.read.csv("/opt/spark-data/sales_data.csv", header=True, inferSchema=True)

# แสดงโครงสร้างของ DataFrame
df.printSchema()

# แสดงข้อมูลใน DataFrame
df.show()

# ลงทะเบียน DataFrame เพื่อใช้งานใน SQL
df.createOrReplaceTempView("sales")

# คำสั่ง SQL สามารถใช้ได้หลายอย่าง เช่น SELECT, FILTER, GROUP BY, ORDER BY, JOIN ฯลฯ
sql_result = spark.sql("SELECT * FROM sales WHERE Sales > 800")
sql_result.show()

