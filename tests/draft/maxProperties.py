
import pytest
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_READ_COMMITTED, ISOLATION_LEVEL_SERIALIZABLE
import json
import os

@pytest.fixture
def db_conn():
    database_url = os.environ.get("DATABASE_URL")
    if database_url is None:
        pytest.fail("DATABASE_URL environment variable is not set.")

    conn = psycopg2.connect(database_url)
    conn.set_isolation_level(ISOLATION_LEVEL_READ_COMMITTED)
    try:
        yield conn
    finally:
        conn.close()


def test_shorter_is_valid(db_conn):
    data = {'foo': 1}
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'maxProperties': 2}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "maxPropertiesvalidation"
        
def test_exact_length_is_valid(db_conn):
    data = {'foo': 1, 'bar': 2}
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'maxProperties': 2}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "maxPropertiesvalidation"
        
def test_too_long_is_invalid(db_conn):
    data = {'foo': 1, 'bar': 2, 'baz': 3}
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'maxProperties': 2}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "maxPropertiesvalidation"
        
def test_ignores_arrays(db_conn):
    data = [1, 2, 3]
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'maxProperties': 2}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "maxPropertiesvalidation"
        
def test_ignores_strings(db_conn):
    data = 'foobar'
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'maxProperties': 2}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "maxPropertiesvalidation"
        
def test_ignores_other_nonobjects(db_conn):
    data = 12
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'maxProperties': 2}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "maxPropertiesvalidation"
        
def test_shorter_is_valid(db_conn):
    data = {'foo': 1}
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'maxProperties': 2.0}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "maxPropertiesvalidationwithadecimal"
        
def test_too_long_is_invalid(db_conn):
    data = {'foo': 1, 'bar': 2, 'baz': 3}
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'maxProperties': 2.0}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "maxPropertiesvalidationwithadecimal"
        
def test_no_properties_is_valid(db_conn):
    data = {}
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'maxProperties': 0}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "maxProperties0meanstheobjectisempty"
        
def test_one_property_is_invalid(db_conn):
    data = {'foo': 1}
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'maxProperties': 0}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "maxProperties0meanstheobjectisempty"
        