from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
from dotenv import load_dotenv
import os
from resources.dev.config import bucket_name


# Load environment variables from the .env file
load_dotenv()

# Adding the packages required to get data from S3
os.environ[
    "PYSPARK_SUBMIT_ARGS"
] = "--packages com.amazonaws:aws-java-sdk-s3:1.12.196,org.apache.hadoop:hadoop-aws:3.3.1 pyspark-shell"

# Creating our Spark configuration
conf = SparkConf().setAppName("S3toSpark")

# Create Spark Context
sc = SparkContext(conf=conf)

# Create Spark Session
spark = SparkSession(sc).builder.appName("S3App").getOrCreate()

# Configure the settings to read from the S3 bucket
hadoopConf = sc._jsc.hadoopConfiguration()

hadoopConf.set("fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY_ID"))
hadoopConf.set("fs.s3a.secret.key", os.getenv("AWS_SECRET_ACCESS_KEY"))
hadoopConf.set(
    "spark.hadoop.fs.s3a.aws.credentials.provider",
    "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
)

# Read from the S3 bucket (update path and file name as needed)
while True:
    df = spark.read.csv(f"s3a://{bucket_name}/sales_data/extra_column_csv_generated_sales_data_2025-03-20.csv")
    df.show(16, True)