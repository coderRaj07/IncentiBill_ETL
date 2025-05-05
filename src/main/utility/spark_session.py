import findspark
from resources.dev import config
findspark.init()
from functools import reduce
import os
import boto3
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from src.main.utility.logging_config import *

# Load environment variables from .env
load_dotenv()

# Set environment variables for required packages
os.environ[
    "PYSPARK_SUBMIT_ARGS"
] = "--packages org.postgresql:postgresql:42.2.18,com.amazonaws:aws-java-sdk-bundle:1.12.262,org.apache.hadoop:hadoop-aws:3.3.4 pyspark-shell"


def create_spark_session():
    """
    Create and return a SparkSession configured for AWS S3 and PostgreSQL.
    """
    spark = SparkSession.builder \
        .appName("SparkWithPostgresAndAWS") \
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY_ID")) \
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("AWS_SECRET_ACCESS_KEY")) \
        .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
        .getOrCreate()

    logger.info("Spark session initialized with S3 + PGSQL support.")
    return spark


def list_csv_files_in_s3(spark, s3_path: str):
    """
    List all CSV files in the given S3 path using Hadoop FileSystem APIs.

    Args:
        spark: SparkSession
        s3_path (str): S3 path to list files from (e.g., s3a://bucket/folder)

    Returns:
        List of full paths to CSV files in the given S3 folder.
    """
    sc = spark.sparkContext
    hadoop_conf = sc._jsc.hadoopConfiguration()
    Path = sc._gateway.jvm.org.apache.hadoop.fs.Path
    FileSystem = sc._gateway.jvm.org.apache.hadoop.fs.FileSystem

    uri = Path(s3_path).toUri()
    fs = FileSystem.get(uri, hadoop_conf)
    status_list = fs.listStatus(Path(s3_path))

    return [
        file_status.getPath().toString()
        for file_status in status_list
        if file_status.isFile() and file_status.getPath().getName().endswith(".csv")
    ]


def validate_and_merge_csvs(spark, csv_paths):
    """
    Validate and merge CSV files based on mandatory columns. 
    Move error files to error location in s3

    Args:
        spark: SparkSession
        csv_paths (List[str]): List of CSV file paths to process.

    Returns:
        Merged DataFrame of valid CSVs, or None if no valid CSVs are found.
    """
    valid_dfs = []
    for csv_path in csv_paths:
        logger.info(f"Checking CSV: {csv_path}")
        df = spark.read.option("header", "true").option("inferSchema", "true").csv(csv_path)
        df_columns = set(df.columns)

        mandatory_columns_ = {*config.mandatory_columns}
        if mandatory_columns_.issubset(df_columns):
            logger.info(f"CSV {csv_path} is valid.")
            valid_dfs.append(df.select(*mandatory_columns_))
        else:
            logger.warning(f"CSV {csv_path} missing columns. Moving to invalid folder.")
            move_to_invalid_folder(csv_path)

    if not valid_dfs:
        return None

    # Merge all valid DataFrames into one using reduce and unionByName
    merged_df = reduce(lambda df1, df2: df1.unionByName(df2), valid_dfs)
    return merged_df


def move_to_invalid_folder(s3_path: str):
    """
    Move a CSV file that failed validation to an 'error' folder in the same S3 bucket.

    Args:
        s3_path (str): Full S3 path of the file to be moved.
    """
    s3 = boto3.resource("s3")

    # The s3_path.split("/") splits the given S3 path string 
    # (e.g., s3://bucket/folder/file.csv) by slashes (/).
    # [-1] grabs the last element from the split path, which would be the filename. 
    # For example, for the path s3://bucket/folder/file.csv, filename would be "file.csv".
    filename = s3_path.split("/")[-1] 

    key_parts = s3_path.split("/", 3)
    if len(key_parts) < 4:
        logger.error(f"Unexpected S3 path: {s3_path}")
        return

    source_key = key_parts[3]
    dest_key = f"{config.s3_error_directory}/{filename}"

    logger.info(f"Moving file from `{source_key}` to `{dest_key}`")

    copy_source = {"Bucket": config.bucket_name, "Key": source_key}
    s3.Object(config.bucket_name, dest_key).copy(copy_source)
    s3.Object(config.bucket_name, source_key).delete()


def write_df_to_pgsql(df, table_name):
    """
    Write the DataFrame to a PostgreSQL table using JDBC.

    Args:
        df: Spark DataFrame to write.
        table_name (str): Target table name in PostgreSQL.
    """
    jdbc_url = os.getenv("DATABASE_URL")
    if not jdbc_url.startswith("jdbc:"):
        jdbc_url = f"jdbc:{jdbc_url}"

    (df.write 
        .format("jdbc") 
        .option("url", jdbc_url) 
        .option("dbtable", table_name) 
        .option("user", os.getenv("DB_USER")) 
        .option("password", os.getenv("DB_PASSWORD")) 
        .option("driver", os.getenv("DB_DRIVER")) 
        .mode("overwrite") 
        .save())

    logger.info(f"Data written to PostgreSQL table `{table_name}` successfully.")


if __name__ == "__main__":
    # Initialize Spark session
    spark = create_spark_session()

    # Define the S3 folder path
    s3_folder_path = f"s3a://{config.bucket_name}/sales_data/"
    logger.info(f"Scanning S3 folder: {s3_folder_path}")

    # List all CSV files in the S3 folder
    csv_paths = list_csv_files_in_s3(spark, s3_folder_path)
    logger.info(f"Found {len(csv_paths)} CSV file(s).")

    # Validate and merge CSV files
    merged_df = validate_and_merge_csvs(spark, csv_paths)

    # Write merged DataFrame to PostgreSQL if available
    if merged_df:
        merged_df.show(5)
        write_df_to_pgsql(merged_df, config.product_staging_table)
    else:
        logger.warning("No valid CSVs found. Nothing written to PostgreSQL.")
