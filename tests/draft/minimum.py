
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


def test_above_the_minimum_is_valid(db_conn):
    data = 2.6
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'minimum': 1.1}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "minimumvalidation"
        
def test_boundary_point_is_valid(db_conn):
    data = 1.1
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'minimum': 1.1}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "minimumvalidation"
        
def test_below_the_minimum_is_invalid(db_conn):
    data = 0.6
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'minimum': 1.1}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "minimumvalidation"
        
def test_ignores_nonnumbers(db_conn):
    data = 'x'
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'minimum': 1.1}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "minimumvalidation"
        
def test_negative_above_the_minimum_is_valid(db_conn):
    data = -1
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'minimum': -2}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "minimumvalidationwithsignedinteger"
        
def test_positive_above_the_minimum_is_valid(db_conn):
    data = 0
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'minimum': -2}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "minimumvalidationwithsignedinteger"
        
def test_boundary_point_is_valid(db_conn):
    data = -2
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'minimum': -2}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "minimumvalidationwithsignedinteger"
        
def test_boundary_point_with_float_is_valid(db_conn):
    data = -2.0
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'minimum': -2}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "minimumvalidationwithsignedinteger"
        
def test_float_below_the_minimum_is_invalid(db_conn):
    data = -2.0001
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'minimum': -2}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "minimumvalidationwithsignedinteger"
        
def test_int_below_the_minimum_is_invalid(db_conn):
    data = -3
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'minimum': -2}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "minimumvalidationwithsignedinteger"
        
def test_ignores_nonnumbers(db_conn):
    data = 'x'
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'minimum': -2}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "minimumvalidationwithsignedinteger"
        