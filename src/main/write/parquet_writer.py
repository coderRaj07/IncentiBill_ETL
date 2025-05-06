import traceback
from src.main.utility.logging_config import *

class ParquetWriter:
    def __init__(self, mode, data_format):
        self.mode = mode
        self.data_format = data_format

    def dataframe_writer(self, df, file_path):
        """
        Write a DataFrame directly to S3 in the specified format.
        :param df: Spark DataFrame
        :param file_path: S3 path in the format s3a://bucket-name/path/
        """
        try:
            logger.info(f"Writing DataFrame to {file_path} in {self.data_format} format with mode={self.mode}")
            df.write.format(self.data_format) \
                .option("header", "true") \
                .mode(self.mode) \
                .save(file_path)
            logger.info("Write operation successful.")
        except Exception as e:
            logger.error(f"Error writing the data to S3: {str(e)}")
            traceback_message = traceback.format_exc()
            print(traceback_message)
            raise e