import pytest
import psycopg2
import json
import os

@pytest.fixture
def db_conn():
    database_url = os.environ.get("DATABASE_URL")
    if database_url is None:
        pytest.fail("DATABASE_URL environment variable is not set.")
    
    conn = psycopg2.connect(database_url)
    try:
        yield conn
    finally:
        conn.close()

def test_valid_with_exclusive_minimum(db_conn):
    valid_data = 6  # This needs to be greater than the exclusiveMinimum to pass
    schema = {
        "type": "number",
        "exclusiveMinimum": 5
    }

    data_str = json.dumps(valid_data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "Validation should succeed when the value is greater than the exclusiveMinimum."

def test_invalid_with_exclusive_minimum_equal(db_conn):
    invalid_data = 5  # This is equal to exclusiveMinimum, hence should fail
    schema = {
        "type": "number",
        "exclusiveMinimum": 5
    }

    data_str = json.dumps(invalid_data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "Validation should fail when the value is equal to the exclusiveMinimum."

def test_invalid_with_exclusive_minimum_below(db_conn):
    invalid_data = 4  # This is below the exclusiveMinimum, hence should fail
    schema = {
        "type": "number",
        "exclusiveMinimum": 5
    }

    data_str = json.dumps(invalid_data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "Validation should fail when the value is below the exclusiveMinimum."

