import psycopg2
from resources.dev.config import database_properties

def get_pgsql_connection():
    connection = psycopg2.connect(
        host=database_properties["host"],
        port=database_properties["port"],
        database=database_properties["database"],
        user=database_properties["user"],
        password=database_properties["password"],
        sslmode=database_properties["sslmode"]
    )
    return connection

# Usage
try:
    connection = get_pgsql_connection()
    print("Connected to PostgreSQL database")

    cursor = connection.cursor()
    cursor.execute("SELECT version();")
    version = cursor.fetchone()
    print("PostgreSQL version:", version)

    cursor.close()
    connection.close()

except Exception as e:
    print("Error connecting to PostgreSQL:", e)