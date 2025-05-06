import findspark
findspark.init()
import os
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