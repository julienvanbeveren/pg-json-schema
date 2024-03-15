import os
import psycopg2
import pytest

# Setting the environment variable (usually this would be set outside your script)
os.environ["DATABASE_URL"] = "postgres://postgres:admin@localhost:5432/db"

@pytest.fixture(scope="session", autouse=True)
def clean_testing_database():
    setup()

def setup():
    print("Setting up the database for testing.")
    database_url = os.environ.get("DATABASE_URL")  # Ensure this environment variable is set
    if database_url is None:
        raise Exception("DATABASE_URL environment variable is not set.")

    # Establish a connection to the database
    conn = psycopg2.connect(database_url)
    try:
        # Open a cursor to perform database operations
        cur = conn.cursor()

        # Read the SQL script
        script_path = os.path.join(os.path.dirname(__file__), "../src/validate_schema.sql")
        with open(script_path, "r") as sql_file:
            sql_script = sql_file.read()

        # Execute the SQL script
        cur.execute(sql_script)
        conn.commit()  # Commit changes
        print("Database setup script executed successfully.")
    except Exception as e:
        print(f"An error occurred during database setup: {e}")
        raise
    finally:
        conn.close()

