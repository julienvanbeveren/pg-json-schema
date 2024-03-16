
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


def test_all_property_names_valid(db_conn):
    data = {'f': {}, 'foo': {}}
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'propertyNames': {'maxLength': 3}}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "propertyNamesvalidation"
        
def test_some_property_names_invalid(db_conn):
    data = {'foo': {}, 'foobar': {}}
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'propertyNames': {'maxLength': 3}}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "propertyNamesvalidation"
        
def test_object_without_properties_is_valid(db_conn):
    data = {}
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'propertyNames': {'maxLength': 3}}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "propertyNamesvalidation"
        
def test_ignores_arrays(db_conn):
    data = [1, 2, 3, 4]
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'propertyNames': {'maxLength': 3}}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "propertyNamesvalidation"
        
def test_ignores_strings(db_conn):
    data = 'foobar'
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'propertyNames': {'maxLength': 3}}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "propertyNamesvalidation"
        
def test_ignores_other_nonobjects(db_conn):
    data = 12
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'propertyNames': {'maxLength': 3}}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "propertyNamesvalidation"
        
def test_object_with_any_properties_is_valid(db_conn):
    data = {'foo': 1}
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'propertyNames': True}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "propertyNameswithbooleanschematrue"
        
def test_empty_object_is_valid(db_conn):
    data = {}
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'propertyNames': True}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "propertyNameswithbooleanschematrue"
        
def test_object_with_any_properties_is_invalid(db_conn):
    data = {'foo': 1}
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'propertyNames': False}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "propertyNameswithbooleanschemafalse"
        
def test_empty_object_is_valid(db_conn):
    data = {}
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'propertyNames': False}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "propertyNameswithbooleanschemafalse"
        