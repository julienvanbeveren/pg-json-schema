
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


def test_first_anyOf_valid(db_conn):
    data = 1
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'anyOf': [{'type': 'integer'}, {'minimum': 2}]}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "anyOf"
        
def test_second_anyOf_valid(db_conn):
    data = 2.5
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'anyOf': [{'type': 'integer'}, {'minimum': 2}]}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "anyOf"
        
def test_both_anyOf_valid(db_conn):
    data = 3
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'anyOf': [{'type': 'integer'}, {'minimum': 2}]}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "anyOf"
        
def test_neither_anyOf_valid(db_conn):
    data = 1.5
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'anyOf': [{'type': 'integer'}, {'minimum': 2}]}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "anyOf"
        
def test_mismatch_base_schema(db_conn):
    data = 3
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'type': 'string', 'anyOf': [{'maxLength': 2}, {'minLength': 4}]}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "anyOfwithbaseschema"
        
def test_one_anyOf_valid(db_conn):
    data = 'foobar'
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'type': 'string', 'anyOf': [{'maxLength': 2}, {'minLength': 4}]}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "anyOfwithbaseschema"
        
def test_both_anyOf_invalid(db_conn):
    data = 'foo'
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'type': 'string', 'anyOf': [{'maxLength': 2}, {'minLength': 4}]}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "anyOfwithbaseschema"
        
def test_any_value_is_valid(db_conn):
    data = 'foo'
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'anyOf': [True, True]}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "anyOfwithbooleanschemasalltrue"
        
def test_any_value_is_valid(db_conn):
    data = 'foo'
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'anyOf': [True, False]}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "anyOfwithbooleanschemassometrue"
        
def test_any_value_is_invalid(db_conn):
    data = 'foo'
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'anyOf': [False, False]}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "anyOfwithbooleanschemasallfalse"
        
def test_first_anyOf_valid_complex(db_conn):
    data = {'bar': 2}
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'anyOf': [{'properties': {'bar': {'type': 'integer'}}, 'required': ['bar']}, {'properties': {'foo': {'type': 'string'}}, 'required': ['foo']}]}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "anyOfcomplextypes"
        
def test_second_anyOf_valid_complex(db_conn):
    data = {'foo': 'baz'}
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'anyOf': [{'properties': {'bar': {'type': 'integer'}}, 'required': ['bar']}, {'properties': {'foo': {'type': 'string'}}, 'required': ['foo']}]}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "anyOfcomplextypes"
        
def test_both_anyOf_valid_complex(db_conn):
    data = {'foo': 'baz', 'bar': 2}
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'anyOf': [{'properties': {'bar': {'type': 'integer'}}, 'required': ['bar']}, {'properties': {'foo': {'type': 'string'}}, 'required': ['foo']}]}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "anyOfcomplextypes"
        
def test_neither_anyOf_valid_complex(db_conn):
    data = {'foo': 2, 'bar': 'quux'}
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'anyOf': [{'properties': {'bar': {'type': 'integer'}}, 'required': ['bar']}, {'properties': {'foo': {'type': 'string'}}, 'required': ['foo']}]}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "anyOfcomplextypes"
        
def test_string_is_valid(db_conn):
    data = 'foo'
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'anyOf': [{'type': 'number'}, {}]}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "anyOfwithoneemptyschema"
        
def test_number_is_valid(db_conn):
    data = 123
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'anyOf': [{'type': 'number'}, {}]}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "anyOfwithoneemptyschema"
        
def test_null_is_valid(db_conn):
    data = None
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'anyOf': [{'anyOf': [{'type': 'null'}]}]}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "nestedanyOftocheckvalidationsemantics"
        
def test_anything_nonnull_is_invalid(db_conn):
    data = 123
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'anyOf': [{'anyOf': [{'type': 'null'}]}]}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "nestedanyOftocheckvalidationsemantics"
        