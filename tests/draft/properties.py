
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


def test_both_properties_present_and_valid_is_valid(db_conn):
    data = {'foo': 1, 'bar': 'baz'}
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'properties': {'foo': {'type': 'integer'}, 'bar': {'type': 'string'}}}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "objectpropertiesvalidation"
        
def test_one_property_invalid_is_invalid(db_conn):
    data = {'foo': 1, 'bar': {}}
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'properties': {'foo': {'type': 'integer'}, 'bar': {'type': 'string'}}}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "objectpropertiesvalidation"
        
def test_both_properties_invalid_is_invalid(db_conn):
    data = {'foo': [], 'bar': {}}
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'properties': {'foo': {'type': 'integer'}, 'bar': {'type': 'string'}}}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "objectpropertiesvalidation"
        
def test_doesnt_invalidate_other_properties(db_conn):
    data = {'quux': []}
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'properties': {'foo': {'type': 'integer'}, 'bar': {'type': 'string'}}}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "objectpropertiesvalidation"
        
def test_ignores_arrays(db_conn):
    data = []
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'properties': {'foo': {'type': 'integer'}, 'bar': {'type': 'string'}}}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "objectpropertiesvalidation"
        
def test_ignores_other_nonobjects(db_conn):
    data = 12
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'properties': {'foo': {'type': 'integer'}, 'bar': {'type': 'string'}}}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "objectpropertiesvalidation"
        
def test_property_validates_property(db_conn):
    data = {'foo': [1, 2]}
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'properties': {'foo': {'type': 'array', 'maxItems': 3}, 'bar': {'type': 'array'}}, 'patternProperties': {'f.o': {'minItems': 2}}, 'additionalProperties': {'type': 'integer'}}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "propertiespatternPropertiesadditionalPropertiesinteraction"
        
def test_property_invalidates_property(db_conn):
    data = {'foo': [1, 2, 3, 4]}
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'properties': {'foo': {'type': 'array', 'maxItems': 3}, 'bar': {'type': 'array'}}, 'patternProperties': {'f.o': {'minItems': 2}}, 'additionalProperties': {'type': 'integer'}}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "propertiespatternPropertiesadditionalPropertiesinteraction"
        
def test_patternProperty_invalidates_property(db_conn):
    data = {'foo': []}
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'properties': {'foo': {'type': 'array', 'maxItems': 3}, 'bar': {'type': 'array'}}, 'patternProperties': {'f.o': {'minItems': 2}}, 'additionalProperties': {'type': 'integer'}}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "propertiespatternPropertiesadditionalPropertiesinteraction"
        
def test_patternProperty_validates_nonproperty(db_conn):
    data = {'fxo': [1, 2]}
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'properties': {'foo': {'type': 'array', 'maxItems': 3}, 'bar': {'type': 'array'}}, 'patternProperties': {'f.o': {'minItems': 2}}, 'additionalProperties': {'type': 'integer'}}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "propertiespatternPropertiesadditionalPropertiesinteraction"
        
def test_patternProperty_invalidates_nonproperty(db_conn):
    data = {'fxo': []}
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'properties': {'foo': {'type': 'array', 'maxItems': 3}, 'bar': {'type': 'array'}}, 'patternProperties': {'f.o': {'minItems': 2}}, 'additionalProperties': {'type': 'integer'}}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "propertiespatternPropertiesadditionalPropertiesinteraction"
        
def test_additionalProperty_ignores_property(db_conn):
    data = {'bar': []}
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'properties': {'foo': {'type': 'array', 'maxItems': 3}, 'bar': {'type': 'array'}}, 'patternProperties': {'f.o': {'minItems': 2}}, 'additionalProperties': {'type': 'integer'}}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "propertiespatternPropertiesadditionalPropertiesinteraction"
        
def test_additionalProperty_validates_others(db_conn):
    data = {'quux': 3}
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'properties': {'foo': {'type': 'array', 'maxItems': 3}, 'bar': {'type': 'array'}}, 'patternProperties': {'f.o': {'minItems': 2}}, 'additionalProperties': {'type': 'integer'}}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "propertiespatternPropertiesadditionalPropertiesinteraction"
        
def test_additionalProperty_invalidates_others(db_conn):
    data = {'quux': 'foo'}
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'properties': {'foo': {'type': 'array', 'maxItems': 3}, 'bar': {'type': 'array'}}, 'patternProperties': {'f.o': {'minItems': 2}}, 'additionalProperties': {'type': 'integer'}}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "propertiespatternPropertiesadditionalPropertiesinteraction"
        
def test_no_property_present_is_valid(db_conn):
    data = {}
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'properties': {'foo': True, 'bar': False}}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "propertieswithbooleanschema"
        
def test_only_true_property_present_is_valid(db_conn):
    data = {'foo': 1}
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'properties': {'foo': True, 'bar': False}}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "propertieswithbooleanschema"
        
def test_only_false_property_present_is_invalid(db_conn):
    data = {'bar': 2}
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'properties': {'foo': True, 'bar': False}}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "propertieswithbooleanschema"
        
def test_both_properties_present_is_invalid(db_conn):
    data = {'foo': 1, 'bar': 2}
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'properties': {'foo': True, 'bar': False}}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "propertieswithbooleanschema"
        
def test_object_with_all_numbers_is_valid(db_conn):
    data = {'foo\nbar': 1, 'foo"bar': 1, 'foo\\bar': 1, 'foo\rbar': 1, 'foo\tbar': 1, 'foo\x0cbar': 1}
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'properties': {'foo\nbar': {'type': 'number'}, 'foo"bar': {'type': 'number'}, 'foo\\bar': {'type': 'number'}, 'foo\rbar': {'type': 'number'}, 'foo\tbar': {'type': 'number'}, 'foo\x0cbar': {'type': 'number'}}}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "propertieswithescapedcharacters"
        
def test_object_with_strings_is_invalid(db_conn):
    data = {'foo\nbar': '1', 'foo"bar': '1', 'foo\\bar': '1', 'foo\rbar': '1', 'foo\tbar': '1', 'foo\x0cbar': '1'}
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'properties': {'foo\nbar': {'type': 'number'}, 'foo"bar': {'type': 'number'}, 'foo\\bar': {'type': 'number'}, 'foo\rbar': {'type': 'number'}, 'foo\tbar': {'type': 'number'}, 'foo\x0cbar': {'type': 'number'}}}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "propertieswithescapedcharacters"
        
def test_allows_null_values(db_conn):
    data = {'foo': None}
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'properties': {'foo': {'type': 'null'}}}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "propertieswithnullvaluedinstanceproperties"
        
def test_ignores_arrays(db_conn):
    data = []
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'properties': {'__proto__': {'type': 'number'}, 'toString': {'properties': {'length': {'type': 'string'}}}, 'constructor': {'type': 'number'}}}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "propertieswhosenamesareJavascriptobjectpropertynames"
        
def test_ignores_other_nonobjects(db_conn):
    data = 12
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'properties': {'__proto__': {'type': 'number'}, 'toString': {'properties': {'length': {'type': 'string'}}}, 'constructor': {'type': 'number'}}}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "propertieswhosenamesareJavascriptobjectpropertynames"
        
def test_none_of_the_properties_mentioned(db_conn):
    data = {}
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'properties': {'__proto__': {'type': 'number'}, 'toString': {'properties': {'length': {'type': 'string'}}}, 'constructor': {'type': 'number'}}}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "propertieswhosenamesareJavascriptobjectpropertynames"
        
def test___proto___not_valid(db_conn):
    data = {'__proto__': 'foo'}
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'properties': {'__proto__': {'type': 'number'}, 'toString': {'properties': {'length': {'type': 'string'}}}, 'constructor': {'type': 'number'}}}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "propertieswhosenamesareJavascriptobjectpropertynames"
        
def test_toString_not_valid(db_conn):
    data = {'toString': {'length': 37}}
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'properties': {'__proto__': {'type': 'number'}, 'toString': {'properties': {'length': {'type': 'string'}}}, 'constructor': {'type': 'number'}}}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "propertieswhosenamesareJavascriptobjectpropertynames"
        
def test_constructor_not_valid(db_conn):
    data = {'constructor': {'length': 37}}
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'properties': {'__proto__': {'type': 'number'}, 'toString': {'properties': {'length': {'type': 'string'}}}, 'constructor': {'type': 'number'}}}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "propertieswhosenamesareJavascriptobjectpropertynames"
        
def test_all_present_and_valid(db_conn):
    data = {'__proto__': 12, 'toString': {'length': 'foo'}, 'constructor': 37}
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'properties': {'__proto__': {'type': 'number'}, 'toString': {'properties': {'length': {'type': 'string'}}}, 'constructor': {'type': 'number'}}}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "propertieswhosenamesareJavascriptobjectpropertynames"
        