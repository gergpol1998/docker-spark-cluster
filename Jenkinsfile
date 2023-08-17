pipeline {
    agent any

    stages {
        stage('Install Docker Compose') {
            steps {
                sh 'curl -L "https://github.com/docker/compose/releases/download/1.29.2/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose'
                sh 'chmod +x /usr/local/bin/docker-compose'
            }
        }

        stage('Build Image') {
            steps {
                sh 'docker build -t cluster-apache-spark:3.0.2 .'
            }
        }

        stage('Docker Compose down') {
            steps {
                sh 'docker-compose down'
            }
        }

        stage('Docker Compose up') {
            steps {
                sh 'docker-compose up -d'
            }
        }
    }
}
