pipeline {
    agent any

    stages {
        
        stage('shell in docker') {
            steps {
                sh 'docker exec -i docker-spark_spark-master_1 bash'
            }
        }
    }
}
