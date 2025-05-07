import os
from dotenv import load_dotenv

# Load environment variables from the .env file
load_dotenv()

key = os.getenv("KEY")
iv = os.getenv("IV")
salt = os.getenv("SALT") 

#AWS Access And Secret key
aws_access_key = os.getenv("ENCRYPTED_AWS_ACCESS_KEY") # "encrypted_access_key"
aws_secret_key = os.getenv("ENCRYPTED_AWS_SECRET_ACCESS_KEY") # "encrypted_secret_key"

#AWS directory
bucket_name = "de-project-testing-aws"
s3_customer_datamart_directory = "customer_data_mart"
s3_sales_datamart_directory = "sales_data_mart"
s3_source_directory = "sales_data/"
s3_error_directory = "sales_data_error/"
s3_processed_directory = "sales_data_processed/"
s3_sales_partitioned_datamart_directory= "sales_partitioned_data_mart/"

#Database credential
database_name = os.getenv("DATABASE_NAME")
url = os.getenv("DATABASE_URL")
database_properties = {
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "driver": os.getenv("DB_DRIVER"),
    "host":os.getenv("DB_HOST"),
    "port":os.getenv("DB_PORT"),
    "database": os.getenv("DATABASE_NAME"),
    "sslmode":os.getenv("DB_SSLMODE")
}

print("database", database_properties)

# Table name
customer_table_name = "customer"
product_staging_table = "product_staging_table"
product_table = "product"
sales_team_table = "sales_team"
store_table = "store"

#Data Mart details
customer_data_mart_table = "customers_data_mart"
sales_team_data_mart_table = "sales_team_data_mart"

# Required columns
mandatory_columns = ["customer_id","store_id","product_name","sales_date","sales_person_id","price","quantity","total_cost"]

# Base directory 
# Automatically detect the project root (i.e., parent of 'resources') where 'resources' and 'download_location' are located
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))

# Spark data folder location
spark_data_directory = os.path.join(BASE_DIR, "download_location", "data_engineering", "spark_data")
sales_data_for_s3 = os.path.join(BASE_DIR, "sales_data_for_s3") # Generate dummy data to here and upload to s3 from here