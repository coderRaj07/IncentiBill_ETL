import os
from resources.dev import config
from src.main.utility.logging_config import *
from src.main.utility.pg_sql_session import get_pgsql_connection

class DatabaseWriter:
    # def __init__(self,url,properties):
    #     self.url = url
    #     self.properties = properties

    # def write_dataframe(self,df,table_name):
    #     try:
    #         print("inside write_dataframe")
    #         df.write.jdbc(url=self.url,
    #                       table=table_name,
    #                       mode="append",
    #                       properties=self.properties)
    #         logger.info(f"Data successfully written into {table_name} table ")
    #     except Exception as e:
    #         return {f"Message: Error occured {e}"}

    def write_file_info_to_pgsql(self, file_info):
        """
        Write file metadata to PostgreSQL using psycopg2. 
        If the table doesn't exist, it will be created.
        If the table exists, it will be updated with new metadata.

        Args:
            file_info (List[Dict]): List of file-level metadata.
        """
        table_name = config.product_staging_table

        try:
            # Establishing a connection to the PostgreSQL database
            conn = get_pgsql_connection()

            # Creating a cursor to interact with the database
            cursor = conn.cursor()

            # SQL query to insert metadata into the table, using "ON CONFLICT" to handle duplicates
            insert_query = f"""
            INSERT INTO {table_name} (file_name, file_location, created_date, updated_date, status)
            VALUES (%s, %s, %s, NOW(), %s);
            """

            # Insert or update each file's metadata into the table
            for file in file_info:
                cursor.execute(insert_query, (
                    file["file_name"], 
                    file["file_location"], 
                    file["created_date"], 
                    file["status"]
                ))

            # Commit the transaction
            conn.commit()
            logger.info(f"File metadata written to PostgreSQL table `{table_name}` successfully.")

        except Exception as e:
            logger.error(f"Error writing file metadata to PostgreSQL: {str(e)}")
            conn.rollback()  # Rollback in case of error
        finally:
            # Close the cursor and connection
            if cursor:
                cursor.close()
            if conn:
                conn.close()

    def write_df_to_pgsql(self, df, table_name):
        """
        Write the DataFrame to a PostgreSQL table using JDBC.

        Args:
            df: Spark DataFrame to write.
            table_name (str): Target table name in PostgreSQL.
        """
        jdbc_url = os.getenv("DATABASE_URL")
        if not jdbc_url.startswith("jdbc:"):
            jdbc_url = f"jdbc:{jdbc_url}"

        (df.write 
            .format("jdbc") 
            .option("url", jdbc_url) 
            .option("dbtable", table_name) 
            .option("user", os.getenv("DB_USER")) 
            .option("password", os.getenv("DB_PASSWORD")) 
            .option("driver", os.getenv("DB_DRIVER")) 
            .mode("append") 
            .save())

        logger.info(f"Data written to PostgreSQL table `{table_name}` successfully.")