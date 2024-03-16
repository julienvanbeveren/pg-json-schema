
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


def test_longer_is_valid(db_conn):
    data = 'foo'
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "minLength": 2
}

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "minLengthvalidation"
        
def test_exact_length_is_valid(db_conn):
    data = 'fo'
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "minLength": 2
}

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "minLengthvalidation"
        
def test_too_short_is_invalid(db_conn):
    data = 'f'
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "minLength": 2
}

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "minLengthvalidation"
        
def test_ignores_nonstrings(db_conn):
    data = 1
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "minLength": 2
}

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "minLengthvalidation"
        
def test_one_grapheme_is_not_long_enough(db_conn):
    data = '💩'
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "minLength": 2
}

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "minLengthvalidation"
        
def test_longer_is_valid(db_conn):
    data = 'foo'
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "minLength": 2.0
}

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "minLengthvalidationwithadecimal"
        
def test_too_short_is_invalid(db_conn):
    data = 'f'
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "minLength": 2.0
}

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "minLengthvalidationwithadecimal"
        