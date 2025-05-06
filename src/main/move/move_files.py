import traceback
from resources.dev import config
import boto3
from src.main.utility.logging_config import *

def move_to_invalid_folder(s3_path: str):
    """
    Move a CSV file that failed validation to an 'error' folder in the same S3 bucket.

    Args:
        s3_path (str): Full S3 path of the file to be moved.
    """
    try:
        s3 = boto3.resource("s3")

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
    
    except Exception as e:
        logger.error(f"Error moving file : {str(e)}")
        traceback_message = traceback.format_exc()
        print(traceback_message)
        raise e