pipeline {
    agent {
        docker { image 'cluster-apache-spark:3.0.2'}
    }
    

    stages {
        stage('Submit Spark Job') {
            steps {
                script {
                    // Run the Spark job in the Docker container
                    sh """
                        docker exec -it docker-spark-cluster-spark-master-1 \\
                                   /opt/spark/bin/spark-submit /opt/spark-apps/basic_etl/job.py
                    """
                }
            }
        }
    }

}
