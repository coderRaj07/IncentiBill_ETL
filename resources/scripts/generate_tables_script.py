import os
from src.main.utility.pg_sql_session import get_pgsql_connection

def run_sql_script(file_name):
    try:
        # Resolve the full path of the SQL file
        script_dir = os.path.dirname(os.path.abspath(__file__))
        file_path = os.path.join(script_dir, file_name)

        connection = get_pgsql_connection()
        print("Connected to PostgreSQL database")

        cursor = connection.cursor()

        with open(file_path, 'r') as sql_file:
            sql_script = sql_file.read()

        cursor.execute(sql_script)
        connection.commit()
        print("SQL script executed successfully.")

        cursor.close()
        connection.close()

    except Exception as e:
        print("Error running SQL script:", e)

# Example usage
if __name__ == "__main__":
    run_sql_script("table_scripts.sql")
