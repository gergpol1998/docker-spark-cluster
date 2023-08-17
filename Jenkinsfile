pipeline {
    agent any

    stages {
        
        stage('spark-submit') {
            steps {
                sh 'docker exec docker-spark-cluster-spark-master-1 /opt/spark/bin/spark-submit \
                --jars /opt/spark-apps/test_db/postgresql-42.6.0.jar \
                /opt/spark-apps/basic_etl/job.py'
            }
        }
    }
}
