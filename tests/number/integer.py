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

def test_valid_number(db_conn):
    valid_data = 3
    schema = {
        "type": "integer",
    }

    data_str = json.dumps(valid_data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "The schema validation for valid data should succeed."


def test_valid_number_zero_fractional(db_conn):
    valid_data = 3.0
    schema = {
        "type": "integer",
    }

    data_str = json.dumps(valid_data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "The schema validation for valid data should succeed."


def test_invalid(db_conn):
    valid_data = 3.2
    schema = {
        "type": "integer",
    }

    data_str = json.dumps(valid_data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "The schema validation for valid data should succeed."
