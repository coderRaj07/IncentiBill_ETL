import os
from src.main.utility.logging_config import *

class DatabaseReader:
    def __init__(self):
        self.jdbc_url = os.getenv("DATABASE_URL")
        if not self.jdbc_url.startswith("jdbc:"):
            self.jdbc_url = f"jdbc:{self.jdbc_url}"
        self.user = os.getenv("DB_USER")
        self.password = os.getenv("DB_PASSWORD")
        self.driver = os.getenv("DB_DRIVER")

    def create_df_from_pgsql(self, spark, table_name):
        """
        Read data from a PostgreSQL table into a Spark DataFrame using JDBC.

        Args:
            spark: SparkSession instance.
            table_name (str): Name of the PostgreSQL table.

        Returns:
            DataFrame: Spark DataFrame containing table data.
        """
        try:
            df = spark.read.format("jdbc") \
                .option("url", self.jdbc_url) \
                .option("dbtable", table_name) \
                .option("user", self.user) \
                .option("password", self.password) \
                .option("driver", self.driver) \
                .load()

            logger.info(f"Data read successfully from PostgreSQL table `{table_name}`.")
            return df

        except Exception as e:
            logger.error(f"Error reading data from PostgreSQL: {str(e)}")
            raise