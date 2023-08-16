pipeline {
    agent any

    stages {
        
        stage('build docker image') {
            steps {
                sh "docker build -t cluster-apache-spark:3.0.2 ."
            }
        }

    }
}
