pipeline {
    agent any
    
    stages {
        stage('Build Docker Image') {
            steps {
                script {
                    docker.build("cluster-apache-spark:3.0.2 .")
                }
            }
        }
        
        stage('Start Docker Container') {
            steps {
                sh 'docker-compose -f docker-compose.yml up -d'
            }
        }
        
        stage('Submit Spark Job') {
            steps {
                script {
                    docker.image('cluster-apache-spark:3.0.2 .').inside("--name docker-spark-cluster") {
                        sh '/opt/spark/bin/spark-submit /opt/spark-apps/basic_etl/job.py'
                    }
                }
            }
        }
    }
    post {
        always {
            script {
                sh 'docker-compose -f docker-compose.yml down'
            }
        }
    }
    
}
