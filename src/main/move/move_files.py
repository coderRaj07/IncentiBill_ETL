import traceback
from resources.dev import config
import boto3
from src.main.utility.logging_config import *


def move_s3_to_s3(s3_client, bucket_name, source_prefix, destination_prefix):
    try:
        response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=source_prefix)

        for obj in response.get('Contents', []):
            source_key = obj['Key']
            destination_key = destination_prefix + source_key[len(source_prefix):]

            s3_client.copy_object(Bucket=bucket_name,
                                  CopySource={'Bucket': bucket_name,
                                              'Key': source_key}, Key=destination_key)

            s3_client.delete_object(Bucket=bucket_name, Key=source_key)
        return f"Data Moved succesfully from {source_prefix} to {destination_prefix}"
    except Exception as e:
        logger.error(f"Error moving file : {str(e)}")
        traceback_message = traceback.format_exc()
        print(traceback_message)
        raise e


def move_s3_to_s3(s3_client, bucket_name, source_prefix, destination_prefix,file_name=None):
    try:
        response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=source_prefix)

        if file_name is None:
            for obj in response.get('Contents', []):
                source_key = obj['Key']
                destination_key = destination_prefix + source_key[len(source_prefix):]

                s3_client.copy_object(Bucket=bucket_name,
                                      CopySource={'Bucket': bucket_name,
                                                  'Key': source_key}, Key=destination_key)

                s3_client.delete_object(Bucket=bucket_name, Key=source_key)
            # return f"Data Moved succesfully from {source_prefix} to {destination_prefix}"
        else:
            for obj in response.get('Contents', []):
                source_key = obj['Key']

                if source_key.endswith(file_name):
                    destination_key = destination_prefix + source_key[len(source_prefix):]

                    s3_client.copy_object(Bucket=bucket_name,
                                          CopySource={'Bucket': bucket_name,
                                                      'Key': source_key}, Key=destination_key)

                    s3_client.delete_object(Bucket=bucket_name, Key=source_key)
                    logger.info(f"Moved file: {source_key} to {destination_key}")
                # else:
                #     logger.info(f"Skipped file: {source_key} as it doesn't match the filename criteria")

        return f"Data Moved successfully from {source_prefix} to {destination_prefix}"
    except Exception as e:
        logger.error(f"Error moving file : {str(e)}")
        traceback_message = traceback.format_exc()
        print(traceback_message)
        raise e

def move_local_to_local():
    pass

def move_to_invalid_folder(s3_path: str):
    """
    Move a CSV file that failed validation to an 'error' folder in the same S3 bucket.

    Args:
        s3_path (str): Full S3 path of the file to be moved.
    """
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
