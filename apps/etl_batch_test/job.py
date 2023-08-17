from constant import spark_inst
from extract import extract
from transform import select_col, filler_col
from load import load
# import paramiko
import time


try:
    # Initiating and Calling SparkSession
    SPARK = spark_inst()


    #### Extract ####

    # Extracting BankChurners data from kafka
    customer_df = extract(SPARK,"CSV", "/opt/spark-data/BankChurners/BankChurners.csv")

    #### Transformation ####

    # filler age 
    customer_df = filler_col(customer_df)

    #select
    customer_df = select_col(customer_df)

    #show dataframe
    show_df = customer_df.show()

    # # Convert DataFrame to CSV format
    # csv_data = customer_df.toPandas().to_csv(index=False)

    # # SFTP connection parameters
    # sftp_host = '192.168.126.131'
    # sftp_port = 22
    # sftp_username = 'fluke'
    # sftp_password = '1234'
    # remote_file_path = '/Document/output_customer_data.csv'

    # # Establish SSH connection
    # ssh = paramiko.SSHClient()
    # ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    # ssh.connect(sftp_host, port=sftp_port, username=sftp_username, password=sftp_password)

    # # Open SFTP session
    # sftp = ssh.open_sftp()

    # #### Load Data ####

    # # Write data to a local temporary file
    # local_temp_file = "temp_output_data.csv"
    # with open(local_temp_file, "w") as local_file:
    #     local_file.write(csv_data)

    # # Upload local file to remote SFTP server
    # sftp.put(local_temp_file, remote_file_path)

    # # Close SFTP session
    # sftp.close()

    # # Close SSH connection
    # ssh.close()

    # # Delete the local temporary file
    # import os
    # os.remove(local_temp_file)

    # # FileSystem
    load("CSV", customer_df , "/opt/spark-data/output/customer.csv")

    time.sleep(10000)


except Exception as e:
    print("An exception occurred:", str(e))

