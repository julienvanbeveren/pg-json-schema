
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


def test_present_required_property_is_valid(db_conn):
    data = {'foo': 1}
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'properties': {'foo': {}, 'bar': {}}, 'required': ['foo']}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "requiredvalidation"
        
def test_nonpresent_required_property_is_invalid(db_conn):
    data = {'bar': 1}
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'properties': {'foo': {}, 'bar': {}}, 'required': ['foo']}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "requiredvalidation"
        
def test_ignores_arrays(db_conn):
    data = []
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'properties': {'foo': {}, 'bar': {}}, 'required': ['foo']}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "requiredvalidation"
        
def test_ignores_strings(db_conn):
    data = ''
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'properties': {'foo': {}, 'bar': {}}, 'required': ['foo']}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "requiredvalidation"
        
def test_ignores_other_nonobjects(db_conn):
    data = 12
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'properties': {'foo': {}, 'bar': {}}, 'required': ['foo']}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "requiredvalidation"
        
def test_not_required_by_default(db_conn):
    data = {}
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'properties': {'foo': {}}}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "requireddefaultvalidation"
        
def test_property_not_required(db_conn):
    data = {}
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'properties': {'foo': {}}, 'required': []}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "requiredwithemptyarray"
        
def test_object_with_all_properties_present_is_valid(db_conn):
    data = {'foo\nbar': 1, 'foo"bar': 1, 'foo\\bar': 1, 'foo\rbar': 1, 'foo\tbar': 1, 'foo\x0cbar': 1}
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'required': ['foo\nbar', 'foo"bar', 'foo\\bar', 'foo\rbar', 'foo\tbar', 'foo\x0cbar']}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "requiredwithescapedcharacters"
        
def test_object_with_some_properties_missing_is_invalid(db_conn):
    data = {'foo\nbar': '1', 'foo"bar': '1'}
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'required': ['foo\nbar', 'foo"bar', 'foo\\bar', 'foo\rbar', 'foo\tbar', 'foo\x0cbar']}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "requiredwithescapedcharacters"
        
def test_ignores_arrays(db_conn):
    data = []
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'required': ['__proto__', 'toString', 'constructor']}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "requiredpropertieswhosenamesareJavascriptobjectpropertynames"
        
def test_ignores_other_nonobjects(db_conn):
    data = 12
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'required': ['__proto__', 'toString', 'constructor']}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "requiredpropertieswhosenamesareJavascriptobjectpropertynames"
        
def test_none_of_the_properties_mentioned(db_conn):
    data = {}
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'required': ['__proto__', 'toString', 'constructor']}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "requiredpropertieswhosenamesareJavascriptobjectpropertynames"
        
def test___proto___present(db_conn):
    data = {'__proto__': 'foo'}
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'required': ['__proto__', 'toString', 'constructor']}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "requiredpropertieswhosenamesareJavascriptobjectpropertynames"
        
def test_toString_present(db_conn):
    data = {'toString': {'length': 37}}
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'required': ['__proto__', 'toString', 'constructor']}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "requiredpropertieswhosenamesareJavascriptobjectpropertynames"
        
def test_constructor_present(db_conn):
    data = {'constructor': {'length': 37}}
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'required': ['__proto__', 'toString', 'constructor']}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "requiredpropertieswhosenamesareJavascriptobjectpropertynames"
        
def test_all_present(db_conn):
    data = {'__proto__': 12, 'toString': {'length': 'foo'}, 'constructor': 37}
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'required': ['__proto__', 'toString', 'constructor']}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "requiredpropertieswhosenamesareJavascriptobjectpropertynames"
        