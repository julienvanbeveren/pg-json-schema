
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


def test_longer_is_valid(db_conn):
    data = [1, 2]
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'minItems': 1}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "minItemsvalidation"
        
def test_exact_length_is_valid(db_conn):
    data = [1]
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'minItems': 1}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "minItemsvalidation"
        
def test_too_short_is_invalid(db_conn):
    data = []
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'minItems': 1}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "minItemsvalidation"
        
def test_ignores_nonarrays(db_conn):
    data = ''
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'minItems': 1}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "minItemsvalidation"
        
def test_longer_is_valid(db_conn):
    data = [1, 2]
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'minItems': 1.0}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "minItemsvalidationwithadecimal"
        
def test_too_short_is_invalid(db_conn):
    data = []
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'minItems': 1.0}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "minItemsvalidationwithadecimal"
        