import traceback
from src.main.utility.logging_config import *

class S3Reader:

    def list_files(self, s3_client, bucket_name,folder_path):
        try:
            response = s3_client.list_objects_v2(Bucket=bucket_name,Prefix=folder_path)
            if 'Contents' in response:
                logger.info("Total files available in folder '%s' of bucket '%s': %s", folder_path, bucket_name, response)
                files = [f"s3://{bucket_name}/{obj['Key']}" for obj in response['Contents'] if
                         not obj['Key'].endswith('/')]
                return files
            else:
                return []
        except Exception as e:
            error_message = f"Error listing files: {e}"
            traceback_message = traceback.format_exc()
            logger.error("Got this error : %s",error_message)
            print(traceback_message)
            raise


################### Directory will also be available if you use this ###########

    # def list_files(self, bucket_name):
    #     try:
    #         response = self.s3_client.list_objects_v2(Bucket=bucket_name)
    #         if 'Contents' in response:
    #             files = [f"s3://{bucket_name}/{obj['Key']}" for obj in response['Contents']]
    #             return files
    #         else:
    #             return []
    #     except Exception as e:
    #         print(f"Error listing files: {e}")
    #         return []

    def list_csv_files_in_s3(self, spark, s3_path: str):
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