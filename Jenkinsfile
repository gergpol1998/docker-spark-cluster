pipeline {
    agent any

    stages {
        
        stage('Submit Spark Application') {
            steps {
                script {
                    sh "docker exec -it  docker-spark-cluster-spark-master-1 bash"
                    def sparkMaster = "spark://spark-master:7077"
                    docker.image("cluster-apache-spark:3.0.2")
                    sh "/opt/spark/bin/spark-submit --master $sparkMaster /opt/spark-apps/basic_etl/job.py"
                }
            }
        }

    }
}
