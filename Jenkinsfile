pipeline {
    agent any

    stages {
        
        stage('spark-submit') {
            steps {
                sh 'docker exec docker-spark-cluster-spark-master-1 /opt/spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.2 \
                    /opt/spark-apps/streaming/spark_streaming.py'
            }
        }
    }
}
