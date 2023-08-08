from pyspark.sql import SparkSession

def my_function(row):
    # ใส่โค้ดของคุณที่ทำงานใน transformation ของ RDD ตรงนี้
    # เช่น ทำการแปลงค่าในแต่ละ row ของ RDD, คำนวณสถิติ, ฯลฯ
    try:
        # ตัวอย่าง: ทำการแปลงค่าในแต่ละ row ของ RDD
        return row * 2
    except Exception as e:
        # กรณีเกิด exception ใน transformation
        print("An exception occurred:", str(e))
        return None

# สร้าง SparkSession
spark = SparkSession.builder.appName("catch_in_worker").getOrCreate()

try:
    # ตัวอย่าง: สร้าง RDD และใช้งานฟังก์ชัน my_function
    rdd = spark.sparkContext.parallelize([1, 2, 3, 4, 5])
    result_rdd = rdd.map(my_function)

    # ตัวอย่าง: พิมพ์ผลลัพธ์ที่ไม่เป็น None
    for result in result_rdd.collect():
        if result is not None:
            print(result)
    
except Exception as e:
    # กรณีเกิด exception ใน driver program
    print("An exception occurred:", str(e))

# ปิด SparkSession
spark.stop()