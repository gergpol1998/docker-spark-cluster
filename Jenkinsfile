pipeline {
    agent any

    stages {
        
        stage('spark-submit') {
            steps {
                sh 'docker exec docker-spark_spark-master_1 /opt/spark/bin/spark-submit /opt/spark-apps/basic_etl/job.py'
            }
        }
    }
}
