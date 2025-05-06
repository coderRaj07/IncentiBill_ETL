import traceback
from src.main.utility.logging_config import *

class S3Reader:
    
    def list_csv_files_in_s3(self, spark, s3_path: str):
        """
        List all CSV files in the given S3 path using Hadoop FileSystem APIs.

        Args:
            spark: SparkSession
            s3_path (str): S3 path to list files from (e.g., s3a://bucket/folder)

        Returns:
            List of full paths to CSV files in the given S3 folder.
        """
        try:
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
        
        except Exception as e:
                error_message = f"Error listing files: {e}"
                traceback_message = traceback.format_exc()
                logger.error("Got this error : %s",error_message)
                print(traceback_message)
                raise