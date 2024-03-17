
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


def test_first_oneOf_valid(db_conn):
    data = 1
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'oneOf': [{'type': 'integer'}, {'minimum': 2}]}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "oneOf"
        
def test_second_oneOf_valid(db_conn):
    data = 2.5
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'oneOf': [{'type': 'integer'}, {'minimum': 2}]}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "oneOf"
        
def test_both_oneOf_valid(db_conn):
    data = 3
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'oneOf': [{'type': 'integer'}, {'minimum': 2}]}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "oneOf"
        
def test_neither_oneOf_valid(db_conn):
    data = 1.5
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'oneOf': [{'type': 'integer'}, {'minimum': 2}]}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "oneOf"
        
def test_mismatch_base_schema(db_conn):
    data = 3
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'type': 'string', 'oneOf': [{'minLength': 2}, {'maxLength': 4}]}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "oneOfwithbaseschema"
        
def test_one_oneOf_valid(db_conn):
    data = 'foobar'
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'type': 'string', 'oneOf': [{'minLength': 2}, {'maxLength': 4}]}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "oneOfwithbaseschema"
        
def test_both_oneOf_valid(db_conn):
    data = 'foo'
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'type': 'string', 'oneOf': [{'minLength': 2}, {'maxLength': 4}]}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "oneOfwithbaseschema"
        
def test_any_value_is_invalid(db_conn):
    data = 'foo'
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'oneOf': [True, True, True]}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "oneOfwithbooleanschemasalltrue"
        
def test_any_value_is_valid(db_conn):
    data = 'foo'
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'oneOf': [True, False, False]}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "oneOfwithbooleanschemasonetrue"
        
def test_any_value_is_invalid(db_conn):
    data = 'foo'
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'oneOf': [True, True, False]}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "oneOfwithbooleanschemasmorethanonetrue"
        
def test_any_value_is_invalid(db_conn):
    data = 'foo'
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'oneOf': [False, False, False]}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "oneOfwithbooleanschemasallfalse"
        
def test_first_oneOf_valid_complex(db_conn):
    data = {'bar': 2}
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'oneOf': [{'properties': {'bar': {'type': 'integer'}}, 'required': ['bar']}, {'properties': {'foo': {'type': 'string'}}, 'required': ['foo']}]}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "oneOfcomplextypes"
        
def test_second_oneOf_valid_complex(db_conn):
    data = {'foo': 'baz'}
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'oneOf': [{'properties': {'bar': {'type': 'integer'}}, 'required': ['bar']}, {'properties': {'foo': {'type': 'string'}}, 'required': ['foo']}]}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "oneOfcomplextypes"
        
def test_both_oneOf_valid_complex(db_conn):
    data = {'foo': 'baz', 'bar': 2}
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'oneOf': [{'properties': {'bar': {'type': 'integer'}}, 'required': ['bar']}, {'properties': {'foo': {'type': 'string'}}, 'required': ['foo']}]}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "oneOfcomplextypes"
        
def test_neither_oneOf_valid_complex(db_conn):
    data = {'foo': 2, 'bar': 'quux'}
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'oneOf': [{'properties': {'bar': {'type': 'integer'}}, 'required': ['bar']}, {'properties': {'foo': {'type': 'string'}}, 'required': ['foo']}]}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "oneOfcomplextypes"
        
def test_one_valid__valid(db_conn):
    data = 'foo'
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'oneOf': [{'type': 'number'}, {}]}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "oneOfwithemptyschema"
        
def test_both_valid__invalid(db_conn):
    data = 123
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'oneOf': [{'type': 'number'}, {}]}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "oneOfwithemptyschema"
        
def test_both_invalid__invalid(db_conn):
    data = {'bar': 2}
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'type': 'object', 'oneOf': [{'required': ['foo', 'bar']}, {'required': ['foo', 'baz']}]}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "oneOfwithrequired"
        
def test_first_valid__valid(db_conn):
    data = {'foo': 1, 'bar': 2}
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'type': 'object', 'oneOf': [{'required': ['foo', 'bar']}, {'required': ['foo', 'baz']}]}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "oneOfwithrequired"
        
def test_second_valid__valid(db_conn):
    data = {'foo': 1, 'baz': 3}
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'type': 'object', 'oneOf': [{'required': ['foo', 'bar']}, {'required': ['foo', 'baz']}]}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "oneOfwithrequired"
        
def test_both_valid__invalid(db_conn):
    data = {'foo': 1, 'bar': 2, 'baz': 3}
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'type': 'object', 'oneOf': [{'required': ['foo', 'bar']}, {'required': ['foo', 'baz']}]}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "oneOfwithrequired"
        
def test_first_oneOf_valid(db_conn):
    data = {'bar': 8}
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'oneOf': [{'properties': {'bar': True, 'baz': True}, 'required': ['bar']}, {'properties': {'foo': True}, 'required': ['foo']}]}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "oneOfwithmissingoptionalproperty"
        
def test_second_oneOf_valid(db_conn):
    data = {'foo': 'foo'}
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'oneOf': [{'properties': {'bar': True, 'baz': True}, 'required': ['bar']}, {'properties': {'foo': True}, 'required': ['foo']}]}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "oneOfwithmissingoptionalproperty"
        
def test_both_oneOf_valid(db_conn):
    data = {'foo': 'foo', 'bar': 8}
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'oneOf': [{'properties': {'bar': True, 'baz': True}, 'required': ['bar']}, {'properties': {'foo': True}, 'required': ['foo']}]}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "oneOfwithmissingoptionalproperty"
        
def test_neither_oneOf_valid(db_conn):
    data = {'baz': 'quux'}
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'oneOf': [{'properties': {'bar': True, 'baz': True}, 'required': ['bar']}, {'properties': {'foo': True}, 'required': ['foo']}]}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "oneOfwithmissingoptionalproperty"
        
def test_null_is_valid(db_conn):
    data = None
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'oneOf': [{'oneOf': [{'type': 'null'}]}]}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "nestedoneOftocheckvalidationsemantics"
        
def test_anything_nonnull_is_invalid(db_conn):
    data = 123
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'oneOf': [{'oneOf': [{'type': 'null'}]}]}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "nestedoneOftocheckvalidationsemantics"
        