pipeline {
    agent any // You can specify the Jenkins agent here based on your requirements

    environment {
        DOCKER_IMAGE = 'cluster-apache-spark:3.0.2'
    }

    stages {
        stage('Build Docker Image') {
            steps {
                script {
                    sh 'docker build -t $DOCKER_IMAGE .'
                }
            }
        }

        stage('Docker Compose') {
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
                    container('spark-container') {
                        sh '/opt/spark/bin/spark-submit /opt/spark-apps/basic_etl/job.py'
                    }
                }
            }
        }
    }

    post {
        always {
            // Clean up after the pipeline finishes, such as stopping and removing the Docker containers if needed
            sh 'docker-compose down'
        }
    }
}
