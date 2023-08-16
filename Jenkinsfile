pipeline {
    agent any // You can specify the Jenkins agent here based on your requirements

    stages{
        stage('Submit Spark Job') {
            steps {
                script {
                    def sparkImage = docker.image('cluster-apache-spark:3.0.2')
                    sparkImage.inside {
                        sh '/opt/spark/bin/spark-submit /opt/spark-apps/basic_etl/job.py'
                    }
                }
            }
        }
    }
    
    post {
        always {
            sh 'docker-compose down'
        }
    }
}
