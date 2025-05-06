import re
import traceback
import boto3
from resources.dev import config
from src.main.utility.logging_config import *

def move_files_to_folder_in_s3(s3_folder_path: str, destination_folder: str):
    """
    Move all files from the specified S3 folder to another folder within the same bucket.

    Args:
        s3_folder_path (str): Full S3 path of the source folder (e.g., s3a://bucket/source-folder/)
        destination_folder (str): Destination folder/key prefix (e.g., "processed/")
    """
    try:
        s3 = boto3.resource("s3")
        s3_client = boto3.client("s3")

        # Normalize and parse S3 path
        if not s3_folder_path.startswith("s3a://") and not s3_folder_path.startswith("s3://"):
            raise ValueError("S3 path must start with s3a:// or s3://")

        match = re.match(r"s3(?:a)?://([^/]+)/(.+)", s3_folder_path)
        if not match:
            logger.error(f"Invalid S3 path format: {s3_folder_path}")
            return

        bucket_name = match.group(1)
        prefix = match.group(2).rstrip("/")

        logger.info(f"Listing files in bucket `{bucket_name}` with prefix `{prefix}`")

        response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)

        if "Contents" not in response or not response["Contents"]:
            logger.info(f"No files found in folder: {s3_folder_path}")
            return

        for obj in response["Contents"]:
            source_key = obj["Key"]
            filename = source_key.split("/")[-1]
            dest_key = f"{destination_folder.rstrip('/')}/{filename}"

            if source_key == dest_key:
                logger.info(f"Skipping file where source and destination are the same: {source_key}")
                continue

            logger.info(f"Moving `{source_key}` → `{dest_key}`")

            copy_source = {"Bucket": bucket_name, "Key": source_key}
            s3.Object(bucket_name, dest_key).copy(copy_source)
            # s3.Object(bucket_name, source_key).delete()

        logger.info("All eligible files moved successfully.")

    except Exception as e:
        logger.error(f"Error moving files: {str(e)}")
        traceback.print_exc()
        raise

def main():
    try:
        s3_source_directory = f"s3a://{config.bucket_name}/{config.s3_source_directory}/"
        destination_folder = config.s3_processed_directory

        logger.info(f"Starting move from {s3_source_directory} to {destination_folder}")
        move_files_to_folder_in_s3(s3_source_directory, destination_folder)
        logger.info("File move completed successfully.")

    except Exception as e:
        logger.error(f"Script failed with error: {str(e)}")
        traceback.print_exc()

if __name__ == "__main__":
    main()