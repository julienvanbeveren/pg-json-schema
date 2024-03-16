
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


def test_allOf(db_conn):
    data = {'foo': 'baz', 'bar': 2}
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'allOf': [{'properties': {'bar': {'type': 'integer'}}, 'required': ['bar']}, {'properties': {'foo': {'type': 'string'}}, 'required': ['foo']}]}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "allOf"
        
def test_mismatch_second(db_conn):
    data = {'foo': 'baz'}
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'allOf': [{'properties': {'bar': {'type': 'integer'}}, 'required': ['bar']}, {'properties': {'foo': {'type': 'string'}}, 'required': ['foo']}]}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "allOf"
        
def test_mismatch_first(db_conn):
    data = {'bar': 2}
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'allOf': [{'properties': {'bar': {'type': 'integer'}}, 'required': ['bar']}, {'properties': {'foo': {'type': 'string'}}, 'required': ['foo']}]}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "allOf"
        
def test_wrong_type(db_conn):
    data = {'foo': 'baz', 'bar': 'quux'}
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'allOf': [{'properties': {'bar': {'type': 'integer'}}, 'required': ['bar']}, {'properties': {'foo': {'type': 'string'}}, 'required': ['foo']}]}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "allOf"
        
def test_valid(db_conn):
    data = {'foo': 'quux', 'bar': 2, 'baz': None}
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'properties': {'bar': {'type': 'integer'}}, 'required': ['bar'], 'allOf': [{'properties': {'foo': {'type': 'string'}}, 'required': ['foo']}, {'properties': {'baz': {'type': 'null'}}, 'required': ['baz']}]}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "allOfwithbaseschema"
        
def test_mismatch_base_schema(db_conn):
    data = {'foo': 'quux', 'baz': None}
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'properties': {'bar': {'type': 'integer'}}, 'required': ['bar'], 'allOf': [{'properties': {'foo': {'type': 'string'}}, 'required': ['foo']}, {'properties': {'baz': {'type': 'null'}}, 'required': ['baz']}]}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "allOfwithbaseschema"
        
def test_mismatch_first_allOf(db_conn):
    data = {'bar': 2, 'baz': None}
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'properties': {'bar': {'type': 'integer'}}, 'required': ['bar'], 'allOf': [{'properties': {'foo': {'type': 'string'}}, 'required': ['foo']}, {'properties': {'baz': {'type': 'null'}}, 'required': ['baz']}]}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "allOfwithbaseschema"
        
def test_mismatch_second_allOf(db_conn):
    data = {'foo': 'quux', 'bar': 2}
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'properties': {'bar': {'type': 'integer'}}, 'required': ['bar'], 'allOf': [{'properties': {'foo': {'type': 'string'}}, 'required': ['foo']}, {'properties': {'baz': {'type': 'null'}}, 'required': ['baz']}]}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "allOfwithbaseschema"
        
def test_mismatch_both(db_conn):
    data = {'bar': 2}
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'properties': {'bar': {'type': 'integer'}}, 'required': ['bar'], 'allOf': [{'properties': {'foo': {'type': 'string'}}, 'required': ['foo']}, {'properties': {'baz': {'type': 'null'}}, 'required': ['baz']}]}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "allOfwithbaseschema"
        
def test_valid(db_conn):
    data = 25
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'allOf': [{'maximum': 30}, {'minimum': 20}]}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "allOfsimpletypes"
        
def test_mismatch_one(db_conn):
    data = 35
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'allOf': [{'maximum': 30}, {'minimum': 20}]}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "allOfsimpletypes"
        
def test_any_value_is_valid(db_conn):
    data = 'foo'
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'allOf': [True, True]}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "allOfwithbooleanschemasalltrue"
        
def test_any_value_is_invalid(db_conn):
    data = 'foo'
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'allOf': [True, False]}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "allOfwithbooleanschemassomefalse"
        
def test_any_value_is_invalid(db_conn):
    data = 'foo'
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'allOf': [False, False]}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "allOfwithbooleanschemasallfalse"
        
def test_any_data_is_valid(db_conn):
    data = 1
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'allOf': [{}]}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "allOfwithoneemptyschema"
        
def test_any_data_is_valid(db_conn):
    data = 1
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'allOf': [{}, {}]}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "allOfwithtwoemptyschemas"
        
def test_number_is_valid(db_conn):
    data = 1
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'allOf': [{}, {'type': 'number'}]}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "allOfwiththefirstemptyschema"
        
def test_string_is_invalid(db_conn):
    data = 'foo'
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'allOf': [{}, {'type': 'number'}]}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "allOfwiththefirstemptyschema"
        
def test_number_is_valid(db_conn):
    data = 1
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'allOf': [{'type': 'number'}, {}]}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "allOfwiththelastemptyschema"
        
def test_string_is_invalid(db_conn):
    data = 'foo'
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'allOf': [{'type': 'number'}, {}]}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "allOfwiththelastemptyschema"
        
def test_null_is_valid(db_conn):
    data = null
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'allOf': [{'allOf': [{'type': 'null'}]}]}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "nestedallOftocheckvalidationsemantics"
        
def test_anything_nonnull_is_invalid(db_conn):
    data = 123
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'allOf': [{'allOf': [{'type': 'null'}]}]}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "nestedallOftocheckvalidationsemantics"
        
def test_allOf_false_anyOf_false_oneOf_false(db_conn):
    data = 1
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'allOf': [{'multipleOf': 2}], 'anyOf': [{'multipleOf': 3}], 'oneOf': [{'multipleOf': 5}]}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "allOfcombinedwithanyOfoneOf"
        
def test_allOf_false_anyOf_false_oneOf_true(db_conn):
    data = 5
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'allOf': [{'multipleOf': 2}], 'anyOf': [{'multipleOf': 3}], 'oneOf': [{'multipleOf': 5}]}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "allOfcombinedwithanyOfoneOf"
        
def test_allOf_false_anyOf_true_oneOf_false(db_conn):
    data = 3
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'allOf': [{'multipleOf': 2}], 'anyOf': [{'multipleOf': 3}], 'oneOf': [{'multipleOf': 5}]}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "allOfcombinedwithanyOfoneOf"
        
def test_allOf_false_anyOf_true_oneOf_true(db_conn):
    data = 15
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'allOf': [{'multipleOf': 2}], 'anyOf': [{'multipleOf': 3}], 'oneOf': [{'multipleOf': 5}]}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "allOfcombinedwithanyOfoneOf"
        
def test_allOf_true_anyOf_false_oneOf_false(db_conn):
    data = 2
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'allOf': [{'multipleOf': 2}], 'anyOf': [{'multipleOf': 3}], 'oneOf': [{'multipleOf': 5}]}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "allOfcombinedwithanyOfoneOf"
        
def test_allOf_true_anyOf_false_oneOf_true(db_conn):
    data = 10
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'allOf': [{'multipleOf': 2}], 'anyOf': [{'multipleOf': 3}], 'oneOf': [{'multipleOf': 5}]}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "allOfcombinedwithanyOfoneOf"
        
def test_allOf_true_anyOf_true_oneOf_false(db_conn):
    data = 6
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'allOf': [{'multipleOf': 2}], 'anyOf': [{'multipleOf': 3}], 'oneOf': [{'multipleOf': 5}]}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "allOfcombinedwithanyOfoneOf"
        
def test_allOf_true_anyOf_true_oneOf_true(db_conn):
    data = 30
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'allOf': [{'multipleOf': 2}], 'anyOf': [{'multipleOf': 3}], 'oneOf': [{'multipleOf': 5}]}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "allOfcombinedwithanyOfoneOf"
        