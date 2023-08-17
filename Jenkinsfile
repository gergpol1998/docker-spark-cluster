pipeline {
    agent any
    
    stages {
        stage('build-image') {
            steps {
                sh 'docker build -t cluster-apache-spark:3.0.2 .'
            }
        }

        stage('docker-compose') {
            steps {
                sh 'docker-compose up -d'
            }
        }
    }

}
