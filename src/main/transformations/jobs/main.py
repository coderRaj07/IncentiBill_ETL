from datetime import datetime
from functools import reduce
import os
import csv
import psycopg2

from resources.dev import config
from src.main.move.move_files import move_to_invalid_folder
from src.main.read.aws_read import S3Reader
from src.main.utility.encrypt_decrypt import *
from src.main.utility.pg_sql_session import get_pgsql_connection
from src.main.utility.s3_client_object import *
from src.main.utility.logging_config import logger
from src.main.utility.spark_session import create_spark_session
from src.main.write.database_write import DatabaseWriter
from pyspark.sql.functions import create_map, lit, col, to_json
from pyspark.sql.types import StringType
from itertools import chain

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

def validate_and_merge_csvs(spark, csv_paths):
    """
    Validate and merge CSV files based on mandatory columns. 
    Move error files to error location in S3 and store extra columns in 'additional_columns'.

    Args:
        spark: SparkSession
        csv_paths (List[str]): List of CSV file paths to process.

    Returns:
        Merged DataFrame of valid CSVs, or None if no valid CSVs are found.
    """
    valid_dfs = []
    file_info = []  # For storing file-level metadata

    for csv_path in csv_paths:
        logger.info(f"Checking CSV: {csv_path}")
        df = spark.read.option("header", "true").option("inferSchema", "true").csv(csv_path)
        df_columns = set(df.columns)

        mandatory_columns_ = {*config.mandatory_columns}
        if mandatory_columns_.issubset(df_columns):
            logger.info(f"CSV {csv_path} is valid.")

            extra_columns = list(df_columns - mandatory_columns_)

            if extra_columns:
                logger.info(f"Found extra columns: {extra_columns}")
                kv_pairs = list(chain.from_iterable((lit(c), col(c).cast(StringType())) for c in extra_columns))
                df = df.withColumn("additional_columns", to_json(create_map(*kv_pairs)))
            else:
                df = df.withColumn("additional_columns", lit(None).cast(StringType()))

            df = df.select(*mandatory_columns_, "additional_columns")
            valid_dfs.append(df)

            # Collect file metadata
            current_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            file_info.append({
                "file_name": csv_path.split("/")[-1],
                "file_location": csv_path,
                "created_date": datetime.now().strftime('%Y-%m-%d'),
                "formatted_date": current_timestamp,
                "status": "A",  # Status 'A' for active/in-progress
            })
        else:
            logger.warning(f"CSV {csv_path} missing columns. Moving to invalid folder.")
            move_to_invalid_folder(csv_path)

    if not valid_dfs:
        return None, file_info

    # Merge all valid DataFrames into one using reduce and unionByName
    merged_df = reduce(lambda df1, df2: df1.unionByName(df2), valid_dfs)
    return merged_df, file_info


if __name__ == "__main__":
    # Initialize Spark session
    spark = create_spark_session()

    # Define the S3 folder path
    s3_folder_path = f"s3a://{config.bucket_name}/sales_data/"
    logger.info(f"Scanning S3 folder: {s3_folder_path}")

    # List all CSV files in the S3 folder
    csv_paths = S3Reader().list_csv_files_in_s3(spark, s3_folder_path)
    logger.info(f"Found {len(csv_paths)} CSV file(s).")

    # Extract file names from paths
    s3_file_names = [path.split("/")[-1] for path in csv_paths]

    # Check if the s3 directory has already a file
    # if file is there then check if the same file is present in the staging area
    # with status as "A". If so then don't delete and try to re-run
    # else give an error and not process the next file

    # Step: Check in DB if any of these files already have status = 'I'
    if s3_file_names:
        formatted_files = ", ".join(f"'{f}'" for f in s3_file_names)

        connection = get_pgsql_connection()
        cursor = connection.cursor()

        statement = f"""SELECT file_name
                        FROM {config.product_staging_table}
                        WHERE file_name IN ({formatted_files}) AND status='I'
                        GROUP BY file_name"""

        logger.info(f"Dynamically created statement: {statement}")
        cursor.execute(statement)
        data = cursor.fetchall()

        if data:
            logger.warning("Previous run failed for some files: %s", [row[0] for row in data])
        else:
            # Proceed with validation and merge
            merged_df, file_info = validate_and_merge_csvs(spark, csv_paths)

            # Write file metadata
            if file_info:
                DatabaseWriter().write_file_info_to_pgsql(file_info)

            # Write data to database
            if merged_df:
                merged_df.show(5)
                # DatabaseWriter().write_df_to_pgsql(merged_df, config.product_staging_table)
            else:
                logger.warning("No valid CSVs found. Nothing written to PostgreSQL.")
    else:
        logger.info("No CSV files found in S3.")