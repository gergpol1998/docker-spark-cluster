pipeline {
    agent any

    stages {
        stage('clone git') {
            steps {
                sh "git clone https://github.com/gergpol1998/docker-spark-cluster.git"
            }
        }

        stage('build docker image') {
            steps {
                sh "docker build -t cluster-apache-spark:3.0.2 ."
            }
        }

    }
}
