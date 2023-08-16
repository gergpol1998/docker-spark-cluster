from pyspark.sql import SparkSession
import paramiko

# Create Spark session
spark = SparkSession.builder.master("local[*]").appName("SFTPWriteExample").getOrCreate()

# Create sample DataFrame
data = [("test", 25), ("test2", 30), ("test3", 28)]
columns = ["name", "age"]
df = spark.createDataFrame(data, columns)

# Convert DataFrame to CSV format
csv_data = df.toPandas().to_csv(index=False)

# SFTP connection parameters
sftp_host = '192.168.126.131'
sftp_port = 22
sftp_username = 'fluke'
sftp_password = '1234'
remote_file_path = '/Document/output_data.csv'

# Establish SSH connection
ssh = paramiko.SSHClient()
ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
ssh.connect(sftp_host, port=sftp_port, username=sftp_username, password=sftp_password)

# Open SFTP session
sftp = ssh.open_sftp()

# Write data to a local temporary file
local_temp_file = "temp_output_data.csv"
with open(local_temp_file, "w") as local_file:
    local_file.write(csv_data)

# Upload local file to remote SFTP server
sftp.put(local_temp_file, remote_file_path)

# Close SFTP session
sftp.close()

# Close SSH connection
ssh.close()

# Delete the local temporary file
import os
os.remove(local_temp_file)

# Stop Spark session
spark.stop()
