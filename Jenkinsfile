pipeline {
    agent {
        docker { image 'cluster-apache-spark:3.0.2'}
    }
    
    stages {
        stage('docker-compose') {
            steps {
                sh 'docker-compose up -d'
            }
        }
    }

}
