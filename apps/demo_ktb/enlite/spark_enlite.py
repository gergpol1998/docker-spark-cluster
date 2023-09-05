from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from dotenv import load_dotenv
import os
import paramiko
import datetime
import argparse


def download_files_from_sftp(host, port, username, password, local_folder, remote_folder=None):
    transport = paramiko.Transport((host, port))
    transport.connect(username=username, password=password)
    
    sftp = paramiko.SFTPClient.from_transport(transport)
    
    # List files in the remote folder
    remote_files = sftp.listdir(remote_folder)
    
    for file_name in remote_files:
        remote_path = os.path.join(remote_folder, file_name)
        local_path = os.path.join(local_folder, file_name)
        
        # Download the file
        sftp.get(remote_path, local_path)
        print("Downloaded:{}".format(file_name))
    
    sftp.close()
    transport.close()

def insert_data_into_postgres(jdbc_url, table_name, properties, df):
    df.write.jdbc(url=jdbc_url, table=table_name, mode='append', properties=properties)

def main():

    # Get the current month and year
    current_month = datetime.datetime.now().strftime("%m")
    current_year = datetime.datetime.now().strftime("%y")

    parser = argparse.ArgumentParser(description="Download files from SFTP server")
    parser.add_argument("--remote-folder", help="Remote folder to download files from")

    args = parser.parse_args()
    
    if args.remote_folder is not None:
        remote_folder = args.remote_folder
        
    else:
        remote_folder = "/sftpuser/test/fraud-enlite-{}".format(current_month)+"-{}".format(current_year)
        
    # config dotenv file
    load_dotenv()

    host = os.getenv("SFTP_HOSTNAME")
    port = 22
    username = os.getenv("SFTP_USERNAME")
    password = os.getenv("SFTP_PASSWORD")
    local_folder = "/opt/spark-data/enlite"
    
    download_files_from_sftp(host, port, username, password, local_folder, remote_folder)

    # Create SparkSession
    spark = SparkSession.builder \
                        .appName("Enlite_Data") \
                        .master('spark://spark-master:7077') \
                        .config("spark.driver.memory","1g") \
                        .config("spark.driver.cores","1")\
                        .config("spark.executor.memory","1g") \
                        .getOrCreate()
    
    # Read company_profile
    company_profile_df = spark.read.csv("/opt/spark-data/enlite/COMPANY_PROFILE.csv", header=True, inferSchema=True)

    # Read company_telephone
    company_tel_df = spark.read.csv("/opt/spark-data/enlite/COMPANY_TELEPHONE.csv", header=True, inferSchema=True)
    
    # Read company_industry
    company_industry_df = spark.read.csv("/opt/spark-data/enlite/COMPANY_INDUSTRY.csv", header=True, inferSchema=True)

    # Read company_business_size
    company_business_size_df = spark.read.csv("/opt/spark-data/enlite/COMPANY_BUSINESS_SIZE.csv", header=True, inferSchema=True)
    
    # Join DataFrames on the 'company_id' column using an inner join
    merged_df = company_profile_df.join(company_tel_df, on="company_id", how="inner") \
        .join(company_industry_df, on="company_id", how="inner") \
        .join(company_business_size_df, on="company_id", how="inner")

    # Get the current year
    current_year = year(current_date())

    # Extract year from 'registration_date'
    merged_df = merged_df.withColumn("year", col("registration_date").substr(7, 4).cast("int"))

    # Calculate 'current_registered_capital'
    merged_df = merged_df.withColumn("number_of_years_in_operation", current_year - col("year"))

    # Define the columns concatenate
    isic_code = ["ISIC_CODE1", "ISIC_CODE2", "ISIC_CODE3", "ISIC_CODE4", "ISIC_CODE5"]

    # Define the columns concatenate
    naics_code = ["NAICS_CODE1","NAICS_CODE2","NAICS_CODE3","NAICS_CODE4","NAICS_CODE5"]

    # Use concat_ws to concatenate with spaces
    merged_df = merged_df.withColumn("industry_group", concat_ws("", *isic_code))

    # Use concat_ws to concatenate with spaces
    merged_df = merged_df.withColumn("tsic_code_year_of_update", concat_ws("", *naics_code))

    # Select specific columns
    select_df = merged_df.select("company_id", "name_th", "name_en", "juristic_id", "registration_type",
                                "company_status", "registered_capital","number_of_years_in_operation","registration_date",
                                "telephone","industry_group","business_size","tsic_code_year_of_update")

    # Rename the 'telephone' column to 'phone_number'
    select_df = select_df.withColumnRenamed("telephone", "phone_number")

    # Convert registration_date from string to date format
    select_df = select_df.withColumn("registration_date", to_date(col("registration_date"), "dd/MM/yyyy"))

    # Add timestamp columns
    valid_df_with_timestamps = select_df.withColumn("created_at", current_timestamp()) \
                                        .withColumn("updated_at", current_timestamp())
    
    
    # # JDBC URL for PostgreSQL
    # jdbc_url = "jdbc:postgresql://172.25.0.18:5432/postgres"
    # properties = {
    #     "user": "postgres",
    #     "password": "postgres",
    #     "driver": "org.postgresql.Driver"
    # }
    # # Table name
    # table = "enlites"
    
    # # Write data to Postgresql
    # insert_data_into_postgres(jdbc_url, table, properties, valid_df_with_timestamps)
    # print("Inserted successfully")

    # Select specific columns for save to intution
    select_df = merged_df.select("name_th", "name_en", "juristic_id", "registration_type",
                                "company_status", "registered_capital","number_of_years_in_operation","registration_date",
                                "telephone","industry_group","business_size","tsic_code_year_of_update")

    # Rename the 'telephone' column to 'phone_number'
    select_df = select_df.withColumnRenamed("telephone", "phone_number")

    # Get the current month and year
    current_month = datetime.datetime.now().strftime("%m")
    current_year = datetime.datetime.now().strftime("%y")

    # Enlite_path for store data
    enlite_path = "fraud-enlite-{}".format(current_month)+"-{}".format(current_year)

    # Specify the file name including the full path
    csv_output_path = "/opt/spark-data/output"

    # Output file name
    output_filename = "/opt/spark-data/output2/"+enlite_path+"/enlite.csv"

    # Write data to csv file
    select_df.coalesce(1).write.mode('overwrite').csv(csv_output_path, header=True)

    # Read enlite data
    enlite_df = spark.read.csv(csv_output_path,header=True, inferSchema=True)
    
    # upload data to intuation
    enlite_df.toPandas().to_csv(output_filename, sep=',', header=True, index=False)
    print("File uploaded successfully")

    # Delete downloaded files from the local folder
    for file_name in os.listdir(local_folder):
        file_path = os.path.join(local_folder, file_name)
        os.remove(file_path)
    
    # Delete csv files from output folder
    for file_name in os.listdir(csv_output_path):
        file_path = os.path.join(csv_output_path, file_name)
        os.remove(file_path)

    # Stop the Spark session
    spark.stop()
    

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print("An error occurred:{}".format(str(e)))
