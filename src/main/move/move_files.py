import re
import traceback
import boto3
from resources.dev import config
from src.main.utility.logging_config import *

def move_files_to_folder_in_s3(s3_path: str, destination_folder: str):
    """
    Move a file from its current S3 location to a specified folder in the same bucket.

    Args:
        s3_path (str): Full S3 path of the file to move (e.g., s3a://bucket/path/to/file.csv)
        destination_folder (str): Destination folder/key prefix (e.g., "error_data/")
    """
    try:
        s3 = boto3.resource("s3")

        # Extract only the object key from the s3a path
        match = re.match(r"s3a://[^/]+/(.+)", s3_path)
        if not match:
            logger.error(f"Invalid S3 path format: {s3_path}")
            return

        source_key = match.group(1)
        filename = source_key.split("/")[-1]
        dest_key = f"{destination_folder.rstrip('/')}/{filename}"

        if source_key == dest_key:
            logger.info(f"Source and destination are the same for: {s3_path}")
            return

        logger.info(f"Moving file from `{source_key}` to `{dest_key}` in bucket `{config.bucket_name}`")

        copy_source = {"Bucket": config.bucket_name, "Key": source_key}
        s3.Object(config.bucket_name, dest_key).copy(copy_source)
        s3.Object(config.bucket_name, source_key).delete()

    except Exception as e:
        logger.error(f"Error moving file: {str(e)}")
        traceback_message = traceback.format_exc()
        print(traceback_message)
        raise e