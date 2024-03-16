
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


def test_shorter_is_valid(db_conn):
    data = [1]
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "maxItems": 2
}

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "maxItemsvalidation"
        
def test_exact_length_is_valid(db_conn):
    data = [1, 2]
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "maxItems": 2
}

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "maxItemsvalidation"
        
def test_too_long_is_invalid(db_conn):
    data = [1, 2, 3]
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "maxItems": 2
}

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "maxItemsvalidation"
        
def test_ignores_nonarrays(db_conn):
    data = 'foobar'
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "maxItems": 2
}

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "maxItemsvalidation"
        
def test_shorter_is_valid(db_conn):
    data = [1]
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "maxItems": 2.0
}

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "maxItemsvalidationwithadecimal"
        
def test_too_long_is_invalid(db_conn):
    data = [1, 2, 3]
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "maxItems": 2.0
}

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "maxItemsvalidationwithadecimal"
        