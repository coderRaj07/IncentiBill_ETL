import traceback
from resources.dev import config
import boto3
from src.main.utility.logging_config import *

def move_file_to_folder_in_s3(s3_path: str, destination_folder: str):
    """
    Move a file from its current S3 path to a specified folder within the same S3 bucket.

    Args:
        s3_path (str): Full S3 path of the file to be moved.
        destination_folder (str): Destination folder (key prefix) within the same bucket.
    """
    try:
        s3 = boto3.resource("s3")

        filename = s3_path.split("/")[-1]

        key_parts = s3_path.split("/", 3)
        if len(key_parts) < 4:
            logger.error(f"Unexpected S3 path: {s3_path}")
            return

        source_key = key_parts[3]
        dest_key = f"{destination_folder.rstrip('/')}/{filename}"

        logger.info(f"Moving file from `{source_key}` to `{dest_key}`")

        copy_source = {"Bucket": config.bucket_name, "Key": source_key}
        s3.Object(config.bucket_name, dest_key).copy(copy_source)
        s3.Object(config.bucket_name, source_key).delete()

    except Exception as e:
        logger.error(f"Error moving file: {str(e)}")
        traceback_message = traceback.format_exc()
        print(traceback_message)
        raise e