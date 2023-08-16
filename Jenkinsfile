pipeline {
    agent any // You can specify the Jenkins agent here based on your requirements

    stages{
        stage('Submit Spark Job') {
            steps {
                script {
                        // Assuming you have already set up the necessary Docker image with Spark and your job script inside it
                    container('docker-spark-cluster') {
                        sh '/opt/spark/bin/spark-submit /opt/spark-apps/basic_etl/job.py'
                    }
                }
            }
        }
    }
    
}


