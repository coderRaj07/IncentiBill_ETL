from datetime import datetime
from functools import reduce
import os
import csv
import psycopg2

from resources.dev import config
from src.main.move.move_files import move_file_to_folder_in_s3
from src.main.read.aws_read import S3Reader
from src.main.read.database_read import DatabaseReader
from src.main.transformations.jobs.dimension_tables_join import dimensions_table_join
from src.main.utility.encrypt_decrypt import *
from src.main.utility.pg_sql_session import get_pgsql_connection
from src.main.utility.s3_client_object import *
from src.main.utility.logging_config import logger
from src.main.utility.spark_session import create_spark_session
from src.main.write.database_write import DatabaseWriter
from pyspark.sql.functions import create_map, lit, col, to_json, expr
from pyspark.sql.types import StringType
from itertools import chain

from src.main.write.parquet_writer import ParquetWriter

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
            move_file_to_folder_in_s3(csv_path, config.s3_error_directory)

    if not valid_dfs:
        return None, file_info

    # Merge all valid DataFrames into one using reduce and unionByName
    merged_df = reduce(lambda df1, df2: df1.unionByName(df2), valid_dfs)
    return merged_df, file_info


if __name__ == "__main__":
    # Initialize Spark session
    spark = create_spark_session()

    # Define the S3 folder path
    s3_source_directory = f"s3a://{config.bucket_name}/{config.s3_source_directory}/"
    logger.info(f"Scanning S3 folder: {s3_source_directory}")

    # List all CSV files in the S3 folder
    csv_paths = S3Reader().list_csv_files_in_s3(spark, s3_source_directory)
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

        # TODO: Make status "A"
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
            
            # move error csv files into error folder
            # return merged csv with additional column details 
            # and correct csv details (to be written db with status 'A')
            merged_df, file_info = validate_and_merge_csvs(spark, csv_paths)

            # TODO: Uncomment
            # Write correct csv details into db with status 'A'
            # if file_info:
            #     DatabaseWriter().write_file_info_to_pgsql(file_info)

            # Write data to database
            if merged_df:
                merged_df.show(5)
                # DatabaseWriter().write_df_to_pgsql(merged_df, config.product_staging_table)
            else:
                logger.warning("No valid CSVs found. Nothing written to PostgreSQL.")

            logger.info("######## Loading customer_table into customer_table_df #########")
            customer_table_df = DatabaseReader().create_df_from_pgsql(spark, config.customer_table_name)

            logger.info("######## Loading product_table into product_table_df #########")
            product_table_df = DatabaseReader().create_df_from_pgsql(spark, config.product_table)

            logger.info("######## Loading product_staging_table into product_staging_table_df #########")
            product_staging_table_df = DatabaseReader().create_df_from_pgsql(spark, config.product_staging_table)

            logger.info("######## Loading sales_team_table into sales_team_table_df #########")
            sales_team_table_df = DatabaseReader().create_df_from_pgsql(spark, config.sales_team_table)

            logger.info("######## Loading store_table into store_table_df #########")
            store_table_df = DatabaseReader().create_df_from_pgsql(spark, config.store_table)

            s3_customer_store_sales_df_join = dimensions_table_join(merged_df, customer_table_df, store_table_df, sales_team_table_df)

            logger.info("######## Final Enriched Data #########")
            s3_customer_store_sales_df_join.show()

            # Write the customer data into customer data mart in parquet format
            # File will be written to local first
            # Move the RAW data to s3 bucket for reporting tool

            logger.info("######## Write data into customer data mart#########")
            final_customer_data_mart_df = (s3_customer_store_sales_df_join
                                           .select("ct.customer_id"
                                                   ,"ct.first_name"
                                                   ,"ct.last_name"
                                                   ,"ct.address"
                                                   ,"ct.pincode"
                                                   ,"phone_number"
                                                   ,"sales_date"
                                                   ,"total_cost")
                                           )
            
            
            logger.info("######## Final data for customer data mart#########")  
            final_customer_data_mart_df.show()

            parquet_writer = ParquetWriter("overwrite", "parquet")
            s3_customer_datamart_directory = f"s3a://{config.bucket_name}/{config.s3_customer_datamart_directory}/"
            parquet_writer.dataframe_writer(final_customer_data_mart_df, s3_customer_datamart_directory)
            


            # Sales Datamart
            logger.info("######## Write data into sales data mart#########")
            final_sales_team_data_mart_df = (s3_customer_store_sales_df_join
                                           .select("store_id"
                                                   ,"sales_person_id"
                                                   ,"sales_person_first_name"
                                                   ,"sales_person_last_name"
                                                   ,"store_manager_name"
                                                   ,"manager_id"
                                                   ,"is_manager"
                                                   ,"sales_person_address"
                                                   ,"sales_date"
                                                   ,"total_cost"
                                                   ,expr("SUBSTRING(sales_date,1,7) as sales_month")
                                                   )
                                           )
            
            s3_sales_datamart_directory = f"s3a://{config.bucket_name}/{config.s3_sales_datamart_directory}/"
            parquet_writer.dataframe_writer(final_sales_team_data_mart_df, s3_sales_datamart_directory)
            
            # Also writing the data into partitions
            s3_sales_partitioned_datamart_directory = f"s3a://{config.bucket_name}/{config.s3_sales_partitioned_datamart_directory}/"
            (final_sales_team_data_mart_df.write.format("parquet")
                                                .option("header","true")
                                                .mode("overwrite")
                                                .partitionBy("sales_month","store_id")
                                                .option("path",s3_sales_partitioned_datamart_directory)
                                                .save()
            )
            
    else:
        logger.info("No CSV files found in S3.")