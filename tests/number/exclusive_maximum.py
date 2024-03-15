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

def test_valid_with_exclusive_maximum(db_conn):
    valid_data = 4  # This needs to be less than the exclusiveMaximum to pass
    schema = {
        "type": "number",
        "exclusiveMaximum": 5
    }

    data_str = json.dumps(valid_data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "Validation should succeed when the value is less than the exclusiveMaximum."

def test_invalid_with_exclusive_maximum_equal(db_conn):
    invalid_data = 5  # This is equal to exclusiveMaximum, hence should fail
    schema = {
        "type": "number",
        "exclusiveMaximum": 5
    }

    data_str = json.dumps(invalid_data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "Validation should fail when the value is equal to the exclusiveMaximum."

def test_invalid_with_exclusive_maximum_above(db_conn):
    invalid_data = 6  # This is above the exclusiveMaximum, hence should fail
    schema = {
        "type": "number",
        "exclusiveMaximum": 5
    }

    data_str = json.dumps(invalid_data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "Validation should fail when the value is above the exclusiveMaximum."

