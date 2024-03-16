
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


def test_an_integer_is_an_integer(db_conn):
    data = 1
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'type': 'integer'}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "integertypematchesintegers"
        
def test_a_float_with_zero_fractional_part_is_an_integer(db_conn):
    data = 1.0
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'type': 'integer'}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "integertypematchesintegers"
        
def test_a_float_is_not_an_integer(db_conn):
    data = 1.1
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'type': 'integer'}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "integertypematchesintegers"
        
def test_a_string_is_not_an_integer(db_conn):
    data = 'foo'
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'type': 'integer'}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "integertypematchesintegers"
        
def test_a_string_is_still_not_an_integer_even_if_it_looks_like_one(db_conn):
    data = '1'
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'type': 'integer'}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "integertypematchesintegers"
        
def test_an_object_is_not_an_integer(db_conn):
    data = {}
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'type': 'integer'}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "integertypematchesintegers"
        
def test_an_array_is_not_an_integer(db_conn):
    data = []
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'type': 'integer'}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "integertypematchesintegers"
        
def test_a_boolean_is_not_an_integer(db_conn):
    data = True
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'type': 'integer'}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "integertypematchesintegers"
        
def test_null_is_not_an_integer(db_conn):
    data = null
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'type': 'integer'}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "integertypematchesintegers"
        
def test_an_integer_is_a_number(db_conn):
    data = 1
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'type': 'number'}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "numbertypematchesnumbers"
        
def test_a_float_with_zero_fractional_part_is_a_number_and_an_integer(db_conn):
    data = 1.0
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'type': 'number'}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "numbertypematchesnumbers"
        
def test_a_float_is_a_number(db_conn):
    data = 1.1
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'type': 'number'}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "numbertypematchesnumbers"
        
def test_a_string_is_not_a_number(db_conn):
    data = 'foo'
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'type': 'number'}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "numbertypematchesnumbers"
        
def test_a_string_is_still_not_a_number_even_if_it_looks_like_one(db_conn):
    data = '1'
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'type': 'number'}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "numbertypematchesnumbers"
        
def test_an_object_is_not_a_number(db_conn):
    data = {}
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'type': 'number'}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "numbertypematchesnumbers"
        
def test_an_array_is_not_a_number(db_conn):
    data = []
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'type': 'number'}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "numbertypematchesnumbers"
        
def test_a_boolean_is_not_a_number(db_conn):
    data = True
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'type': 'number'}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "numbertypematchesnumbers"
        
def test_null_is_not_a_number(db_conn):
    data = null
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'type': 'number'}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "numbertypematchesnumbers"
        
def test_1_is_not_a_string(db_conn):
    data = 1
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'type': 'string'}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "stringtypematchesstrings"
        
def test_a_float_is_not_a_string(db_conn):
    data = 1.1
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'type': 'string'}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "stringtypematchesstrings"
        
def test_a_string_is_a_string(db_conn):
    data = 'foo'
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'type': 'string'}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "stringtypematchesstrings"
        
def test_a_string_is_still_a_string_even_if_it_looks_like_a_number(db_conn):
    data = '1'
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'type': 'string'}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "stringtypematchesstrings"
        
def test_an_empty_string_is_still_a_string(db_conn):
    data = ''
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'type': 'string'}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "stringtypematchesstrings"
        
def test_an_object_is_not_a_string(db_conn):
    data = {}
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'type': 'string'}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "stringtypematchesstrings"
        
def test_an_array_is_not_a_string(db_conn):
    data = []
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'type': 'string'}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "stringtypematchesstrings"
        
def test_a_boolean_is_not_a_string(db_conn):
    data = True
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'type': 'string'}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "stringtypematchesstrings"
        
def test_null_is_not_a_string(db_conn):
    data = null
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'type': 'string'}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "stringtypematchesstrings"
        
def test_an_integer_is_not_an_object(db_conn):
    data = 1
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'type': 'object'}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "objecttypematchesobjects"
        
def test_a_float_is_not_an_object(db_conn):
    data = 1.1
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'type': 'object'}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "objecttypematchesobjects"
        
def test_a_string_is_not_an_object(db_conn):
    data = 'foo'
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'type': 'object'}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "objecttypematchesobjects"
        
def test_an_object_is_an_object(db_conn):
    data = {}
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'type': 'object'}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "objecttypematchesobjects"
        
def test_an_array_is_not_an_object(db_conn):
    data = []
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'type': 'object'}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "objecttypematchesobjects"
        
def test_a_boolean_is_not_an_object(db_conn):
    data = True
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'type': 'object'}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "objecttypematchesobjects"
        
def test_null_is_not_an_object(db_conn):
    data = null
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'type': 'object'}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "objecttypematchesobjects"
        
def test_an_integer_is_not_an_array(db_conn):
    data = 1
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'type': 'array'}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "arraytypematchesarrays"
        
def test_a_float_is_not_an_array(db_conn):
    data = 1.1
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'type': 'array'}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "arraytypematchesarrays"
        
def test_a_string_is_not_an_array(db_conn):
    data = 'foo'
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'type': 'array'}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "arraytypematchesarrays"
        
def test_an_object_is_not_an_array(db_conn):
    data = {}
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'type': 'array'}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "arraytypematchesarrays"
        
def test_an_array_is_an_array(db_conn):
    data = []
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'type': 'array'}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "arraytypematchesarrays"
        
def test_a_boolean_is_not_an_array(db_conn):
    data = True
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'type': 'array'}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "arraytypematchesarrays"
        
def test_null_is_not_an_array(db_conn):
    data = null
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'type': 'array'}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "arraytypematchesarrays"
        
def test_an_integer_is_not_a_boolean(db_conn):
    data = 1
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'type': 'boolean'}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "booleantypematchesbooleans"
        
def test_zero_is_not_a_boolean(db_conn):
    data = 0
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'type': 'boolean'}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "booleantypematchesbooleans"
        
def test_a_float_is_not_a_boolean(db_conn):
    data = 1.1
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'type': 'boolean'}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "booleantypematchesbooleans"
        
def test_a_string_is_not_a_boolean(db_conn):
    data = 'foo'
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'type': 'boolean'}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "booleantypematchesbooleans"
        
def test_an_empty_string_is_not_a_boolean(db_conn):
    data = ''
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'type': 'boolean'}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "booleantypematchesbooleans"
        
def test_an_object_is_not_a_boolean(db_conn):
    data = {}
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'type': 'boolean'}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "booleantypematchesbooleans"
        
def test_an_array_is_not_a_boolean(db_conn):
    data = []
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'type': 'boolean'}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "booleantypematchesbooleans"
        
def test_true_is_a_boolean(db_conn):
    data = True
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'type': 'boolean'}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "booleantypematchesbooleans"
        
def test_false_is_a_boolean(db_conn):
    data = false
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'type': 'boolean'}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "booleantypematchesbooleans"
        
def test_null_is_not_a_boolean(db_conn):
    data = null
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'type': 'boolean'}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "booleantypematchesbooleans"
        
def test_an_integer_is_not_null(db_conn):
    data = 1
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'type': 'null'}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "nulltypematchesonlythenullobject"
        
def test_a_float_is_not_null(db_conn):
    data = 1.1
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'type': 'null'}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "nulltypematchesonlythenullobject"
        
def test_zero_is_not_null(db_conn):
    data = 0
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'type': 'null'}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "nulltypematchesonlythenullobject"
        
def test_a_string_is_not_null(db_conn):
    data = 'foo'
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'type': 'null'}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "nulltypematchesonlythenullobject"
        
def test_an_empty_string_is_not_null(db_conn):
    data = ''
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'type': 'null'}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "nulltypematchesonlythenullobject"
        
def test_an_object_is_not_null(db_conn):
    data = {}
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'type': 'null'}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "nulltypematchesonlythenullobject"
        
def test_an_array_is_not_null(db_conn):
    data = []
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'type': 'null'}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "nulltypematchesonlythenullobject"
        
def test_true_is_not_null(db_conn):
    data = True
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'type': 'null'}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "nulltypematchesonlythenullobject"
        
def test_false_is_not_null(db_conn):
    data = false
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'type': 'null'}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "nulltypematchesonlythenullobject"
        
def test_null_is_null(db_conn):
    data = null
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'type': 'null'}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "nulltypematchesonlythenullobject"
        
def test_an_integer_is_valid(db_conn):
    data = 1
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'type': ['integer', 'string']}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "multipletypescanbespecifiedinanarray"
        
def test_a_string_is_valid(db_conn):
    data = 'foo'
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'type': ['integer', 'string']}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "multipletypescanbespecifiedinanarray"
        
def test_a_float_is_invalid(db_conn):
    data = 1.1
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'type': ['integer', 'string']}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "multipletypescanbespecifiedinanarray"
        
def test_an_object_is_invalid(db_conn):
    data = {}
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'type': ['integer', 'string']}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "multipletypescanbespecifiedinanarray"
        
def test_an_array_is_invalid(db_conn):
    data = []
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'type': ['integer', 'string']}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "multipletypescanbespecifiedinanarray"
        
def test_a_boolean_is_invalid(db_conn):
    data = True
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'type': ['integer', 'string']}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "multipletypescanbespecifiedinanarray"
        
def test_null_is_invalid(db_conn):
    data = null
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'type': ['integer', 'string']}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "multipletypescanbespecifiedinanarray"
        
def test_string_is_valid(db_conn):
    data = 'foo'
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'type': ['string']}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "typeasarraywithoneitem"
        
def test_number_is_invalid(db_conn):
    data = 123
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'type': ['string']}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "typeasarraywithoneitem"
        
def test_array_is_valid(db_conn):
    data = [1, 2, 3]
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'type': ['array', 'object']}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "typearrayorobject"
        
def test_object_is_valid(db_conn):
    data = {'foo': 123}
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'type': ['array', 'object']}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "typearrayorobject"
        
def test_number_is_invalid(db_conn):
    data = 123
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'type': ['array', 'object']}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "typearrayorobject"
        
def test_string_is_invalid(db_conn):
    data = 'foo'
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'type': ['array', 'object']}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "typearrayorobject"
        
def test_null_is_invalid(db_conn):
    data = null
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'type': ['array', 'object']}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "typearrayorobject"
        
def test_array_is_valid(db_conn):
    data = [1, 2, 3]
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'type': ['array', 'object', 'null']}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "typearrayobjectornull"
        
def test_object_is_valid(db_conn):
    data = {'foo': 123}
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'type': ['array', 'object', 'null']}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "typearrayobjectornull"
        
def test_null_is_valid(db_conn):
    data = null
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'type': ['array', 'object', 'null']}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "typearrayobjectornull"
        
def test_number_is_invalid(db_conn):
    data = 123
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'type': ['array', 'object', 'null']}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "typearrayobjectornull"
        
def test_string_is_invalid(db_conn):
    data = 'foo'
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'type': ['array', 'object', 'null']}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "typearrayobjectornull"
        