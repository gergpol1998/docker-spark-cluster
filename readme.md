#submit jobs

/opt/spark/bin/spark-submit --master spark://spark-master:7077 \
--jars /opt/spark-apps/postgresql-42.2.22.jar \
--driver-memory 1G \
--executor-memory 1G \
/opt/spark-apps/main.py

#submit with kafka
/opt/spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.2 \
--master spark://spark-master:7077 \
--driver-memory 1G \
--executor-memory 1G \
/opt/spark-apps/spark_kafka.py


#เข้า shell kafka
docker exec -it kafka1 sh -c "cd /opt/kafka/bin && /bin/bash"

#สร้าง Topic
./kafka-topics.sh --create --zookeeper 172.25.0.11:2181 --replication-factor 1 --partitions 1 --topic my-topic

#ดู Topic ที่สร้างไปแล้ว
./kafka-topics.sh --bootstrap-server=172.25.0.12:9092 --list

#การ Produce message
./kafka-console-producer.sh  --broker-list 172.25.0.12:9092  --topic my-topic

#การลบ Topics
./kafka-topics.sh --bootstrap-server 172.25.0.12:9092 --topic my-topic --delete






