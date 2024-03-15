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

def test_integer_multiple_of_success(db_conn):
    valid_data = 10
    schema = {
        "type": "number",
        "multipleOf": 5
    }

    data_str = json.dumps(valid_data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "The integer schema validation for multipleOf should succeed."

def test_integer_multiple_of_failure(db_conn):
    invalid_data = 7
    schema = {
        "type": "number",
        "multipleOf": 5
    }

    data_str = json.dumps(invalid_data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "The integer schema validation for multipleOf should fail."

def test_decimal_multiple_of_success(db_conn):
    valid_data = 2.5
    schema = {
        "type": "number",
        "multipleOf": 0.5
    }

    data_str = json.dumps(valid_data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "The decimal schema validation for multipleOf should succeed."

def test_decimal_multiple_of_failure(db_conn):
    invalid_data = 2.3
    schema = {
        "type": "number",
        "multipleOf": 0.5
    }

    data_str = json.dumps(invalid_data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "The decimal schema validation for multipleOf should fail."

