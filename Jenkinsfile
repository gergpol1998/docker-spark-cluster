pipeline {
    agent {
        docker { image 'cluster-apache-spark:3.0.2'}
    }
    
    stages {
        stage('docker-compose') {
            steps {
                script {
                    // Run the Spark job in the Docker container
                    sh """
                        docker-compose up -d 
                    """
                }
            }
        }
    }

}
