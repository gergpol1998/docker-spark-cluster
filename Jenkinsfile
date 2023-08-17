pipeline {
    agent any

    stages {
        
        stage('shell in docker') {
            steps {
                sh 'docker exec docker-spark_spark-master_1 sh'
            }
        }

        stage('spark-submit job') {
            steps {
                sh '/opt/spark/bin/spark-submit /opt/spark-apps/basic_etl/job.py'
            }
        }
    }
}
