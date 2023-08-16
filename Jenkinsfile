pipeline {
    agent docker  // You can specify the Jenkins agent here based on your requirements

    stages {
        stage('build docker image') {
            steps {
                script {
                    sh 'docker build -t cluster-apache-spark:3.0.2 .'
                }
            }
        }

        stage('docker-compose') {
            steps {
                script {
                    sh 'docker-compose up -d'
                }
            }
        }

        stage('Submit Spark Job') {
            steps {
                script {
                    // Assuming you have already set up the necessary Docker image with Spark and your job script inside it
                    docker.image('cluster-apache-spark:3.0.2') .inside {
                        sh '/opt/spark/bin/spark-submit /opt/spark-apps/basic_etl/job.py'
                    }
                }
            }
        }
    }
}
