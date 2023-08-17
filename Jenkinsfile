pipeline {
    agent any

    stages {
        
        stage('spark-submit') {
            steps {
                sh 'docker exec docker-spark-cluster-spark-master-1 /opt/spark/bin/spark-submit /opt/spark-apps/test_sparkjob_ui/test.py'
            }
        }
    }
}
