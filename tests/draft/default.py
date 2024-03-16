
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


def test_valid_when_property_is_specified(db_conn):
    data = {'foo': 13}
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'properties': {'foo': {'type': 'integer', 'default': []}}}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "invalidtypefordefault"
        
def test_still_valid_when_the_invalid_default_is_used(db_conn):
    data = {}
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'properties': {'foo': {'type': 'integer', 'default': []}}}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "invalidtypefordefault"
        
def test_valid_when_property_is_specified(db_conn):
    data = {'bar': 'good'}
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'properties': {'bar': {'type': 'string', 'minLength': 4, 'default': 'bad'}}}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "invalidstringvaluefordefault"
        
def test_still_valid_when_the_invalid_default_is_used(db_conn):
    data = {}
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'properties': {'bar': {'type': 'string', 'minLength': 4, 'default': 'bad'}}}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "invalidstringvaluefordefault"
        
def test_an_explicit_property_value_is_checked_against_maximum_passing(db_conn):
    data = {'alpha': 1}
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'type': 'object', 'properties': {'alpha': {'type': 'number', 'maximum': 3, 'default': 5}}}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "thedefaultkeyworddoesnotdoanythingifthepropertyismissing"
        
def test_an_explicit_property_value_is_checked_against_maximum_failing(db_conn):
    data = {'alpha': 5}
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'type': 'object', 'properties': {'alpha': {'type': 'number', 'maximum': 3, 'default': 5}}}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "thedefaultkeyworddoesnotdoanythingifthepropertyismissing"
        
def test_missing_properties_are_not_filled_in_with_the_default(db_conn):
    data = {}
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'type': 'object', 'properties': {'alpha': {'type': 'number', 'maximum': 3, 'default': 5}}}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "thedefaultkeyworddoesnotdoanythingifthepropertyismissing"
        