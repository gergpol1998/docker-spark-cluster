from pyspark.sql import SparkSession
import paramiko

# Create Spark session
spark = SparkSession.builder.master("local[*]").appName("SFTPExample").getOrCreate()

# SFTP connection parameters
sftp_host = '192.168.126.131'
sftp_port = 22
sftp_username = 'fluke'
sftp_password = '1234'
remote_file_path = '/Document/customer_data.csv'

# Establish SSH connection
ssh = paramiko.SSHClient()
ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
ssh.connect(sftp_host, port=sftp_port, username=sftp_username, password=sftp_password)

# Open SFTP session
sftp = ssh.open_sftp()

# Read the content of the remote file
remote_file = sftp.file(remote_file_path, 'r')
data = remote_file.read().decode()

# Close the remote file
remote_file.close()

# Close SFTP session
sftp.close()

# Close SSH connection
ssh.close()

# Create a Spark DataFrame from the data
data_df = spark.createDataFrame([(data,)], ['content'])  # Wrap 'data' in a tuple

# Show the DataFrame
data_df.show(truncate=False)  # Display full content of 'content' column

# Stop Spark session
spark.stop()
