pipeline {
    agent any

    stages {
        stage('Checkout') {
            steps {
                checkout scm
            }
        }

        stage('Build Docker Image') {
            steps {
                script {
                    def imageName = "cluster-apache-spark:3.0.2"
                    docker.build(imageName, '-f Dockerfile .')
                }
            }
        }

        stage('Docker-Compose') {
            steps {
                script {
                    sh "docker-compose up -d"
                }
            }
        }

        stage('Submit Spark Application') {
            steps {
                script {
                    def sparkCommand = "/opt/spark/bin/spark-submit /opt/spark-apps/basic_etl/job.py"
                    docker.image("cluster-apache-spark:3.0.2")
                        .inside("--network=172.25.0.14") {
                            sh "$sparkCommand"
                        }
                }
            }
        }
    }

    post {
        always {
            sh "docker-compose down"
        }
    }
}
