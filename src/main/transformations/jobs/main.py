import os
import csv
import psycopg2

from resources.dev import config
from src.main.read.aws_read import S3Reader
from src.main.utility.encrypt_decrypt import *
from src.main.utility.pg_sql_session import get_pgsql_connection
from src.main.utility.s3_client_object import *
from src.main.utility.logging_config import logger

aws_access_key = config.aws_access_key
aws_secret_key = config.aws_secret_key

s3_client_provider = S3ClientProvider(decrypt(aws_access_key), decrypt(aws_secret_key))
s3_client = s3_client_provider.get_client()

response = s3_client.list_buckets()
print(response)

logger.info("List of buckets: %s", response["Buckets"])


def is_csv_empty(file_path):
    """Helper function to check if a CSV file is empty."""
    try:
        with open(file_path, newline='', encoding='utf-8') as f:
            reader = csv.reader(f)
            return not any(reader)  # Returns True if the file is empty
    except Exception as e:
        logger.error(f"Error reading file {file_path}: {e}")
        return True  # If there's an error, consider it empty


# Check if the local directory has already a file
# if file is there then check if the same file is present in the staging area
# with status as "A". If so then don't delete and try to re-run
# else give an error and not process the next file

csv_files = [file for file in os.listdir(config.local_directory) if file.endswith(".csv")]

connection = get_pgsql_connection()
cursor = connection.cursor()

total_csv_files = []

if csv_files:

    # Create a list of all csv files
    for file in csv_files:
        file_path = os.path.join(config.local_directory, file)
        
        # if is_csv_empty(file_path):
        #     logger.info(f"CSV file {file} is empty. No record match.")
        #     continue  # Skip the empty file and move to the next one

        # Only add non-empty files
        total_csv_files.append(file)    # append adds single element to the list 
                                        # += adds a list with another list

    # Check if table exists; if not, create it
    # cursor.execute(f"""
    #     SELECT EXISTS (
    #         SELECT FROM information_schema.tables 
    #         WHERE table_schema = '{config.database_name}' 
    #         AND table_name = '{config.product_staging_table}'
    #     );
    # """)
    # table_exists = cursor.fetchone()[0]

    # if not table_exists:
    #     logger.warning(f"Table {config.database_name}.{config.product_staging_table} does not exist. Creating it.")
    #     cursor.execute(f"""
    #         CREATE TABLE {config.database_name}.{config.product_staging_table} (
    #             file_name TEXT,
    #             status CHAR(1)
    #             -- add other columns as per your requirement
    #         );
    #     """)
    #     connection.commit()
    #     logger.info("Table created successfully.")
    
    # Finally
    # total_csv_files = ['file1.csv', 'file2.csv', 'file3.csv']
    # str(total_csv_files) = "['file1.csv', 'file2.csv', 'file3.csv']"

    # from 1st index (inclusive) to -2 index(inclusive) or say -1 index (exclusive)
    # str(total_csv_files)[1:-1] = "'file1.csv', 'file2.csv', 'file3.csv'" 

    # statement = f"""select distinct file_name 
                    # from de_project.product_staging_table
                    # where file_name in ({str(total_csv_files)[1:-1]}) and status='I'"""

    if total_csv_files:
        formatted_files = ", ".join(f"'{f}'" for f in total_csv_files)

        # Since we should avoid using distinct  
        statement = f"""select file_name
                        from {config.database_name}.{config.product_staging_table}
                        where file_name in ({formatted_files}) and status='I'
                        group by file_name"""
        
        logger.info(f"Dynamically created statement: {statement}")
        cursor.execute(statement)
        data = cursor.fetchall()
        
        if data:
            logger.info("Your last run was failed, please check")
        else:
            logger.info("No data found on database")
    else:
        logger.info("No non-empty CSV files to process")

else:
    logger.info("Last run was successful!!!")


try: 
    s3_reader = S3Reader()
    # Bucket name should come from table
    folder_path = config.s3_source_directory
    s3_absolute_file_path = s3_reader.list_files(s3_client, config.bucket_name, folder_path=folder_path)
    logger.info(f"Absolute path on s3 bucket for csv file, {s3_absolute_file_path}")
    if not s3_absolute_file_path:
        logger.info(f"No files available at {folder_path}")
        raise Exception("No data available to process")
    
except Exception as e:
    logger.error(f"Exited with error: {e}")
    raise e