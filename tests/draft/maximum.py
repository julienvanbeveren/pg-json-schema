
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


def test_below_the_maximum_is_valid(db_conn):
    data = 2.6
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "maximum": 3.0
}

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "maximumvalidation"
        
def test_boundary_point_is_valid(db_conn):
    data = 3.0
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "maximum": 3.0
}

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "maximumvalidation"
        
def test_above_the_maximum_is_invalid(db_conn):
    data = 3.5
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "maximum": 3.0
}

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "maximumvalidation"
        
def test_ignores_nonnumbers(db_conn):
    data = 'x'
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "maximum": 3.0
}

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "maximumvalidation"
        
def test_below_the_maximum_is_invalid(db_conn):
    data = 299.97
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "maximum": 300
}

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "maximumvalidationwithunsignedinteger"
        
def test_boundary_point_integer_is_valid(db_conn):
    data = 300
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "maximum": 300
}

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "maximumvalidationwithunsignedinteger"
        
def test_boundary_point_float_is_valid(db_conn):
    data = 300.0
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "maximum": 300
}

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "maximumvalidationwithunsignedinteger"
        
def test_above_the_maximum_is_invalid(db_conn):
    data = 300.5
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "maximum": 300
}

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "maximumvalidationwithunsignedinteger"
        