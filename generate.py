import os
import shutil
import json
import re

source_directory = './tests/jsons'
destination_directory = './tests/draft'

def remove_null_characters(obj):
    """Recursively remove null characters from all string values in a JSON-like object."""
    if isinstance(obj, dict):
        return {k: remove_null_characters(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [remove_null_characters(item) for item in obj]
    elif isinstance(obj, str):
        return obj.replace('\x00', '')
    else:
        return obj

os.makedirs(destination_directory, exist_ok=True)

for filename in os.listdir(source_directory):
    if filename.endswith('.json'):

        new_file_content = """
import pytest
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_READ_COMMITTED, ISOLATION_LEVEL_SERIALIZABLE
import json
import os

@pytest.fixture
def db_conn():
    database_url = os.environ.get("DATABASE_URL")
    if database_url is None:
        pytest.fail("DATABASE_URL environment variable is not set.")

    conn = psycopg2.connect(database_url)
    conn.set_isolation_level(ISOLATION_LEVEL_READ_COMMITTED)
    try:
        yield conn
    finally:
        conn.close()

"""

        source_file_path = os.path.join(source_directory, filename)

        with open(source_file_path, 'r', encoding='utf-8') as json_file:
            json_data = json.load(json_file)

        for suite in json_data:
            schema = suite["schema"]
            schema = remove_null_characters(schema)
            for test in suite["tests"]:
                description_sanitized = "_".join(test["description"].split(" "))
                description_sanitized = re.sub(r'[^0-9a-zA-Z_]', '', description_sanitized)  # Remove special characters
                valid_str = "True" if test["valid"] else "False"

                # Prepare the data for insertion
                if isinstance(test["data"], str):
                    data_repr = f"'{test['data']}'"
                elif isinstance(test["data"], bool):
                    data_repr = "True" if test['data'] else "False"
                elif test["data"] is None:
                    data_repr = "None"

                else:
                    data_repr = test["data"] or json.dumps(test["data"])  # Convert other types to JSON string

                new_file_content += f"""
def test_{description_sanitized}(db_conn):
    data = {data_repr}
    schema = {schema}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is {valid_str}, \"{re.sub(r'[^0-9a-zA-Z_]', '', suite["description"])}\"
        """

        new_file_content = new_file_content.encode("utf-8").replace(b'\x00', b'').decode("utf-8")

        new_filename = os.path.splitext(filename)[0] + '.py'
        destination_file_path = os.path.join(destination_directory, new_filename)

        with open(destination_file_path, 'w', encoding='utf-8') as new_file:
            new_file.write(new_file_content)

        print(f'Created {destination_file_path}')

