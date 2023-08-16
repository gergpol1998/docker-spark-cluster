pipeline {
    agent docker

    stages {
        stage('shell to docker spark') {
            steps {
                script {
                    sh 'docker exec -it docker-spark-cluster-spark-master-1 bash'
                }
            }
        }

        stage('Submit Spark Job') {
            steps {
                script {
                    sh '/opt/spark/bin/spark-submit /opt/spark-apps/basic_etl/job.py'
                }
            }
        }
    }
}