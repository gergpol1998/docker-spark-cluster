from pyspark.sql import SparkSession

# สร้าง SparkSession เพื่อเริ่มต้นใช้งาน PySpark
spark = SparkSession.builder \
    .appName("WordCount") \
    .getOrCreate()

# ตัวอย่างข้อมูลใน RDD
text_data = ["Hello Spark", "Spark is great", "PySpark is awesome", "Hello PySpark"]

# สร้าง RDD โดยใช้ parallelize เพื่อแบ่งข้อมูลให้กลายเป็น RDD
rdd = spark.sparkContext.parallelize(text_data)

# นับคำใน RDD ด้วยการใช้การแยกคำ (split) และการนับคำ
word_count_rdd = rdd.flatMap(lambda line: line.split(" ")).map(lambda word: (word, 1)).reduceByKey(lambda a, b: a + b)

# แสดงผลลัพธ์
word_count_list = word_count_rdd.collect()
for word, count in word_count_list:
    print(f"{word}: {count}")

# ปิด SparkSession เมื่อสิ้นสุดการใช้งาน
spark.stop()
