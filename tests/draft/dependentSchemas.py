
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


def test_valid(db_conn):
    data = {'foo': 1, 'bar': 2}
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'dependentSchemas': {'bar': {'properties': {'foo': {'type': 'integer'}, 'bar': {'type': 'integer'}}}}}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "singledependency"
        
def test_no_dependency(db_conn):
    data = {'foo': 'quux'}
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'dependentSchemas': {'bar': {'properties': {'foo': {'type': 'integer'}, 'bar': {'type': 'integer'}}}}}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "singledependency"
        
def test_wrong_type(db_conn):
    data = {'foo': 'quux', 'bar': 2}
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'dependentSchemas': {'bar': {'properties': {'foo': {'type': 'integer'}, 'bar': {'type': 'integer'}}}}}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "singledependency"
        
def test_wrong_type_other(db_conn):
    data = {'foo': 2, 'bar': 'quux'}
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'dependentSchemas': {'bar': {'properties': {'foo': {'type': 'integer'}, 'bar': {'type': 'integer'}}}}}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "singledependency"
        
def test_wrong_type_both(db_conn):
    data = {'foo': 'quux', 'bar': 'quux'}
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'dependentSchemas': {'bar': {'properties': {'foo': {'type': 'integer'}, 'bar': {'type': 'integer'}}}}}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "singledependency"
        
def test_ignores_arrays(db_conn):
    data = ['bar']
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'dependentSchemas': {'bar': {'properties': {'foo': {'type': 'integer'}, 'bar': {'type': 'integer'}}}}}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "singledependency"
        
def test_ignores_strings(db_conn):
    data = 'foobar'
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'dependentSchemas': {'bar': {'properties': {'foo': {'type': 'integer'}, 'bar': {'type': 'integer'}}}}}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "singledependency"
        
def test_ignores_other_nonobjects(db_conn):
    data = 12
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'dependentSchemas': {'bar': {'properties': {'foo': {'type': 'integer'}, 'bar': {'type': 'integer'}}}}}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "singledependency"
        
def test_object_with_property_having_schema_true_is_valid(db_conn):
    data = {'foo': 1}
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'dependentSchemas': {'foo': True, 'bar': False}}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "booleansubschemas"
        
def test_object_with_property_having_schema_false_is_invalid(db_conn):
    data = {'bar': 2}
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'dependentSchemas': {'foo': True, 'bar': False}}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "booleansubschemas"
        
def test_object_with_both_properties_is_invalid(db_conn):
    data = {'foo': 1, 'bar': 2}
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'dependentSchemas': {'foo': True, 'bar': False}}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "booleansubschemas"
        
def test_empty_object_is_valid(db_conn):
    data = {}
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'dependentSchemas': {'foo': True, 'bar': False}}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "booleansubschemas"
        
def test_quoted_tab(db_conn):
    data = {'foo\tbar': 1, 'a': 2, 'b': 3, 'c': 4}
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'dependentSchemas': {'foo\tbar': {'minProperties': 4}, "foo'bar": {'required': ['foo"bar']}}}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "dependencieswithescapedcharacters"
        
def test_quoted_quote(db_conn):
    data = {"foo'bar": {'foo"bar': 1}}
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'dependentSchemas': {'foo\tbar': {'minProperties': 4}, "foo'bar": {'required': ['foo"bar']}}}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "dependencieswithescapedcharacters"
        
def test_quoted_tab_invalid_under_dependent_schema(db_conn):
    data = {'foo\tbar': 1, 'a': 2}
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'dependentSchemas': {'foo\tbar': {'minProperties': 4}, "foo'bar": {'required': ['foo"bar']}}}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "dependencieswithescapedcharacters"
        
def test_quoted_quote_invalid_under_dependent_schema(db_conn):
    data = {"foo'bar": 1}
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'dependentSchemas': {'foo\tbar': {'minProperties': 4}, "foo'bar": {'required': ['foo"bar']}}}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "dependencieswithescapedcharacters"
        
def test_matches_root(db_conn):
    data = {'foo': 1}
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'properties': {'foo': {}}, 'dependentSchemas': {'foo': {'properties': {'bar': {}}, 'additionalProperties': False}}}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "dependentsubschemaincompatiblewithroot"
        
def test_matches_dependency(db_conn):
    data = {'bar': 1}
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'properties': {'foo': {}}, 'dependentSchemas': {'foo': {'properties': {'bar': {}}, 'additionalProperties': False}}}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "dependentsubschemaincompatiblewithroot"
        
def test_matches_both(db_conn):
    data = {'foo': 1, 'bar': 2}
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'properties': {'foo': {}}, 'dependentSchemas': {'foo': {'properties': {'bar': {}}, 'additionalProperties': False}}}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "dependentsubschemaincompatiblewithroot"
        
def test_no_dependency(db_conn):
    data = {'baz': 1}
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'properties': {'foo': {}}, 'dependentSchemas': {'foo': {'properties': {'bar': {}}, 'additionalProperties': False}}}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "dependentsubschemaincompatiblewithroot"
        