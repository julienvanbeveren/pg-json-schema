
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


def test_correct_types(db_conn):
    data = [1, 'foo']
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'prefixItems': [{'type': 'integer'}, {'type': 'string'}]}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "aschemagivenforprefixItems"
        
def test_wrong_types(db_conn):
    data = ['foo', 1]
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'prefixItems': [{'type': 'integer'}, {'type': 'string'}]}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "aschemagivenforprefixItems"
        
def test_incomplete_array_of_items(db_conn):
    data = [1]
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'prefixItems': [{'type': 'integer'}, {'type': 'string'}]}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "aschemagivenforprefixItems"
        
def test_array_with_additional_items(db_conn):
    data = [1, 'foo', True]
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'prefixItems': [{'type': 'integer'}, {'type': 'string'}]}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "aschemagivenforprefixItems"
        
def test_empty_array(db_conn):
    data = []
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'prefixItems': [{'type': 'integer'}, {'type': 'string'}]}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "aschemagivenforprefixItems"
        
def test_JavaScript_pseudoarray_is_valid(db_conn):
    data = {'0': 'invalid', '1': 'valid', 'length': 2}
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'prefixItems': [{'type': 'integer'}, {'type': 'string'}]}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "aschemagivenforprefixItems"
        
def test_array_with_one_item_is_valid(db_conn):
    data = [1]
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'prefixItems': [True, False]}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "prefixItemswithbooleanschemas"
        
def test_array_with_two_items_is_invalid(db_conn):
    data = [1, 'foo']
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'prefixItems': [True, False]}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "prefixItemswithbooleanschemas"
        
def test_empty_array_is_valid(db_conn):
    data = []
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'prefixItems': [True, False]}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "prefixItemswithbooleanschemas"
        
def test_only_the_first_item_is_validated(db_conn):
    data = [1, 'foo', False]
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'prefixItems': [{'type': 'integer'}]}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "additionalitemsareallowedbydefault"
        
def test_allows_null_elements(db_conn):
    data = [None]
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'prefixItems': [{'type': 'null'}]}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "prefixItemswithnullinstanceelements"
        