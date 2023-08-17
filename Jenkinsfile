pipeline {
    agent any
    
    stages {
        stage('Test Docker') {
            steps {
                script {
                    def dockerVersion = docker.version()
                    echo "Docker version: ${dockerVersion}"
                }
            }
        }
    }
}