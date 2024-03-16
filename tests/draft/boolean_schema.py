
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


def test_number_is_valid(db_conn):
    data = 1
    schema = true

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "booleanschematrue"
        
def test_string_is_valid(db_conn):
    data = 'foo'
    schema = true

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "booleanschematrue"
        
def test_boolean_true_is_valid(db_conn):
    data = true
    schema = true

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "booleanschematrue"
        
def test_boolean_false_is_valid(db_conn):
    data = false
    schema = true

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "booleanschematrue"
        
def test_null_is_valid(db_conn):
    data = null
    schema = true

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "booleanschematrue"
        
def test_object_is_valid(db_conn):
    data = {"foo": "bar"}
    schema = true

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "booleanschematrue"
        
def test_empty_object_is_valid(db_conn):
    data = {}
    schema = true

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "booleanschematrue"
        
def test_array_is_valid(db_conn):
    data = ["foo"]
    schema = true

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "booleanschematrue"
        
def test_empty_array_is_valid(db_conn):
    data = []
    schema = true

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "booleanschematrue"
        
def test_number_is_invalid(db_conn):
    data = 1
    schema = false

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "booleanschemafalse"
        
def test_string_is_invalid(db_conn):
    data = 'foo'
    schema = false

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "booleanschemafalse"
        
def test_boolean_true_is_invalid(db_conn):
    data = true
    schema = false

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "booleanschemafalse"
        
def test_boolean_false_is_invalid(db_conn):
    data = false
    schema = false

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "booleanschemafalse"
        
def test_null_is_invalid(db_conn):
    data = null
    schema = false

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "booleanschemafalse"
        
def test_object_is_invalid(db_conn):
    data = {"foo": "bar"}
    schema = false

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "booleanschemafalse"
        
def test_empty_object_is_invalid(db_conn):
    data = {}
    schema = false

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "booleanschemafalse"
        
def test_array_is_invalid(db_conn):
    data = ["foo"]
    schema = false

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "booleanschemafalse"
        
def test_empty_array_is_invalid(db_conn):
    data = []
    schema = false

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "booleanschemafalse"
        