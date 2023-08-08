from pyspark.sql import SparkSession

try:
    # สร้าง SparkSession (หากยังไม่ได้สร้าง)
    spark = SparkSession.builder.appName("log_error").getOrCreate()

    # ระดับของ Log ที่ต้องการเขียน (เช่น "ERROR", "WARN", "INFO", "DEBUG")
    log_level = "ERROR"

    # ตั้งค่าระดับของ Log ของ SparkContext
    spark.sparkContext.setLogLevel(log_level)

    # ตั้งค่าเส้นทางที่ต้องการเก็บ Log file
    log_file_path = "/opt/spark-data/log/logfile.txt"

    # สร้างตัวบันทึก (logger) โดยใช้ SparkContext
    logger = spark.sparkContext._jvm.org.apache.log4j
    logger.LogManager.getLogger("org").setLevel(logger.Level.ERROR)

    # สร้าง Spark DataFrame สำหรับการทดสอบ
    data = [("Alice", 1), ("Bob", 2), ("Charlie", 3)]
    df = spark.createDataFrame(data, ["name", "age"])

    # ตัวอย่างการเขียน Log ไปยังไฟล์
    df.select("Product")
    df.write.mode("overwrite").csv(log_file_path)

    # ปิด SparkSession
    spark.stop()

except Exception as e:
    # กรณีเกิดข้อผิดพลาดในการทำงานของ Spark
    print("An error occurred:", str(e))
