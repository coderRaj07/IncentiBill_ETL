import findspark
from resources.dev import config
findspark.init()

import os
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from dotenv import load_dotenv
from resources.dev.config import bucket_name
from src.main.utility.logging_config import *

# Load environment variables from .env
load_dotenv()

# Add required packages
os.environ[
    "PYSPARK_SUBMIT_ARGS"
] = "--packages org.postgresql:postgresql:42.2.18,com.amazonaws:aws-java-sdk-bundle:1.12.262,org.apache.hadoop:hadoop-aws:3.3.4 pyspark-shell"

def create_spark_session():
    conf = SparkConf().setAppName("SparkWithPostgresAndAWS")
    sc = SparkContext(conf=conf)
    spark = SparkSession(sc).builder.getOrCreate()

    # AWS S3 config
    hadoopConf = sc._jsc.hadoopConfiguration()
    hadoopConf.set("fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY_ID"))
    hadoopConf.set("fs.s3a.secret.key", os.getenv("AWS_SECRET_ACCESS_KEY"))
    hadoopConf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    hadoopConf.set(
        "spark.hadoop.fs.s3a.aws.credentials.provider",
        "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
    )

    logger.info("Spark session initialized with S3 + PGSQL support.")
    return spark

def read_pgsql_table(spark, table_name):
    jdbc_url = os.getenv("DATABASE_URL")  # Already in JDBC format
    return spark.read \
        .format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", table_name) \
        .option("user", os.getenv("DB_USER")) \
        .option("password", os.getenv("DB_PASSWORD")) \
        .option("driver", os.getenv("DB_DRIVER")) \
        .load()

def write_df_to_pgsql(df, table_name):
    raw_url = os.getenv("DATABASE_URL")
    jdbc_url = raw_url if raw_url.startswith("jdbc:") else f"jdbc:{raw_url}"

    (
        df.write
        .format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", table_name)
        .option("user", os.getenv("DB_USER"))
        .option("password", os.getenv("DB_PASSWORD"))
        .option("driver", os.getenv("DB_DRIVER"))
        .option("createTableOptions", "WITH (OIDS=FALSE)")  # optional: table creation settings
        .mode("overwrite")  # creates table + writes
        .save()
    )
    logger.info(f"Data written to PostgreSQL table `{table_name}` successfully.")


if __name__ == "__main__":
    spark = create_spark_session()

    # Path to CSV in S3
    s3_csv_path = f"s3a://{bucket_name}/sales_data/generated_csv_sales_data.csv"
    logger.info(f"Reading CSV from: {s3_csv_path}")

    # Read CSV from S3
    df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv(s3_csv_path)

    df.show(5)
    df.printSchema()

    # Write to PostgreSQL
    write_df_to_pgsql(df, config.product_staging_table)
