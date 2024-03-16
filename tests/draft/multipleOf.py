
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


def test_int_by_int(db_conn):
    data = 10
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "multipleOf": 2
}

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "byint"
        
def test_int_by_int_fail(db_conn):
    data = 7
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "multipleOf": 2
}

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "byint"
        
def test_ignores_nonnumbers(db_conn):
    data = 'foo'
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "multipleOf": 2
}

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "byint"
        
def test_zero_is_multiple_of_anything(db_conn):
    data = 0
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "multipleOf": 1.5
}

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "bynumber"
        
def test_45_is_multiple_of_15(db_conn):
    data = 4.5
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "multipleOf": 1.5
}

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "bynumber"
        
def test_35_is_not_multiple_of_15(db_conn):
    data = 35
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "multipleOf": 1.5
}

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "bynumber"
        
def test_00075_is_multiple_of_00001(db_conn):
    data = 0.0075
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "multipleOf": 0.0001
}

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "bysmallnumber"
        
def test_000751_is_not_multiple_of_00001(db_conn):
    data = 0.00751
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "multipleOf": 0.0001
}

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "bysmallnumber"
        
def test_always_invalid_but_naive_implementations_may_raise_an_overflow_error(db_conn):
    data = 1e+308
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "type": "integer",
    "multipleOf": 0.123456789
}

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "floatdivisioninf"
        
def test_any_integer_is_a_multiple_of_1e8(db_conn):
    data = 12391239123
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "type": "integer",
    "multipleOf": 1e-08
}

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "smallmultipleoflargeinteger"
        