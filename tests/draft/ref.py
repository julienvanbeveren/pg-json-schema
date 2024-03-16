
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


def test_match(db_conn):
    data = {'foo': False}
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'properties': {'foo': {'$ref': '#'}}, 'additionalProperties': False}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "rootpointerref"
        
def test_recursive_match(db_conn):
    data = {'foo': {'foo': False}}
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'properties': {'foo': {'$ref': '#'}}, 'additionalProperties': False}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "rootpointerref"
        
def test_mismatch(db_conn):
    data = {'bar': False}
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'properties': {'foo': {'$ref': '#'}}, 'additionalProperties': False}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "rootpointerref"
        
def test_recursive_mismatch(db_conn):
    data = {'foo': {'bar': False}}
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'properties': {'foo': {'$ref': '#'}}, 'additionalProperties': False}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "rootpointerref"
        
def test_match(db_conn):
    data = {'bar': 3}
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'properties': {'foo': {'type': 'integer'}, 'bar': {'$ref': '#/properties/foo'}}}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "relativepointerreftoobject"
        
def test_mismatch(db_conn):
    data = {'bar': True}
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'properties': {'foo': {'type': 'integer'}, 'bar': {'$ref': '#/properties/foo'}}}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "relativepointerreftoobject"
        
def test_match_array(db_conn):
    data = [1, 2]
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'prefixItems': [{'type': 'integer'}, {'$ref': '#/prefixItems/0'}]}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "relativepointerreftoarray"
        
def test_mismatch_array(db_conn):
    data = [1, 'foo']
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'prefixItems': [{'type': 'integer'}, {'$ref': '#/prefixItems/0'}]}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "relativepointerreftoarray"
        
def test_slash_invalid(db_conn):
    data = {'slash': 'aoeu'}
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', '$defs': {'tilde~field': {'type': 'integer'}, 'slash/field': {'type': 'integer'}, 'percent%field': {'type': 'integer'}}, 'properties': {'tilde': {'$ref': '#/$defs/tilde~0field'}, 'slash': {'$ref': '#/$defs/slash~1field'}, 'percent': {'$ref': '#/$defs/percent%25field'}}}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "escapedpointerref"
        
def test_tilde_invalid(db_conn):
    data = {'tilde': 'aoeu'}
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', '$defs': {'tilde~field': {'type': 'integer'}, 'slash/field': {'type': 'integer'}, 'percent%field': {'type': 'integer'}}, 'properties': {'tilde': {'$ref': '#/$defs/tilde~0field'}, 'slash': {'$ref': '#/$defs/slash~1field'}, 'percent': {'$ref': '#/$defs/percent%25field'}}}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "escapedpointerref"
        
def test_percent_invalid(db_conn):
    data = {'percent': 'aoeu'}
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', '$defs': {'tilde~field': {'type': 'integer'}, 'slash/field': {'type': 'integer'}, 'percent%field': {'type': 'integer'}}, 'properties': {'tilde': {'$ref': '#/$defs/tilde~0field'}, 'slash': {'$ref': '#/$defs/slash~1field'}, 'percent': {'$ref': '#/$defs/percent%25field'}}}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "escapedpointerref"
        
def test_slash_valid(db_conn):
    data = {'slash': 123}
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', '$defs': {'tilde~field': {'type': 'integer'}, 'slash/field': {'type': 'integer'}, 'percent%field': {'type': 'integer'}}, 'properties': {'tilde': {'$ref': '#/$defs/tilde~0field'}, 'slash': {'$ref': '#/$defs/slash~1field'}, 'percent': {'$ref': '#/$defs/percent%25field'}}}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "escapedpointerref"
        
def test_tilde_valid(db_conn):
    data = {'tilde': 123}
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', '$defs': {'tilde~field': {'type': 'integer'}, 'slash/field': {'type': 'integer'}, 'percent%field': {'type': 'integer'}}, 'properties': {'tilde': {'$ref': '#/$defs/tilde~0field'}, 'slash': {'$ref': '#/$defs/slash~1field'}, 'percent': {'$ref': '#/$defs/percent%25field'}}}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "escapedpointerref"
        
def test_percent_valid(db_conn):
    data = {'percent': 123}
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', '$defs': {'tilde~field': {'type': 'integer'}, 'slash/field': {'type': 'integer'}, 'percent%field': {'type': 'integer'}}, 'properties': {'tilde': {'$ref': '#/$defs/tilde~0field'}, 'slash': {'$ref': '#/$defs/slash~1field'}, 'percent': {'$ref': '#/$defs/percent%25field'}}}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "escapedpointerref"
        
def test_nested_ref_valid(db_conn):
    data = 5
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', '$defs': {'a': {'type': 'integer'}, 'b': {'$ref': '#/$defs/a'}, 'c': {'$ref': '#/$defs/b'}}, '$ref': '#/$defs/c'}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "nestedrefs"
        
def test_nested_ref_invalid(db_conn):
    data = 'a'
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', '$defs': {'a': {'type': 'integer'}, 'b': {'$ref': '#/$defs/a'}, 'c': {'$ref': '#/$defs/b'}}, '$ref': '#/$defs/c'}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "nestedrefs"
        
def test_ref_valid_maxItems_valid(db_conn):
    data = {'foo': []}
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', '$defs': {'reffed': {'type': 'array'}}, 'properties': {'foo': {'$ref': '#/$defs/reffed', 'maxItems': 2}}}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "refappliesalongsidesiblingkeywords"
        
def test_ref_valid_maxItems_invalid(db_conn):
    data = {'foo': [1, 2, 3]}
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', '$defs': {'reffed': {'type': 'array'}}, 'properties': {'foo': {'$ref': '#/$defs/reffed', 'maxItems': 2}}}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "refappliesalongsidesiblingkeywords"
        
def test_ref_invalid(db_conn):
    data = {'foo': 'string'}
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', '$defs': {'reffed': {'type': 'array'}}, 'properties': {'foo': {'$ref': '#/$defs/reffed', 'maxItems': 2}}}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "refappliesalongsidesiblingkeywords"
        
def test_remote_ref_valid(db_conn):
    data = {'minLength': 1}
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', '$ref': 'https://json-schema.org/draft/2020-12/schema'}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "remoterefcontainingrefsitself"
        
def test_remote_ref_invalid(db_conn):
    data = {'minLength': -1}
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', '$ref': 'https://json-schema.org/draft/2020-12/schema'}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "remoterefcontainingrefsitself"
        
def test_property_named_ref_valid(db_conn):
    data = {'$ref': 'a'}
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'properties': {'$ref': {'type': 'string'}}}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "propertynamedrefthatisnotareference"
        
def test_property_named_ref_invalid(db_conn):
    data = {'$ref': 2}
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'properties': {'$ref': {'type': 'string'}}}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "propertynamedrefthatisnotareference"
        
def test_property_named_ref_valid(db_conn):
    data = {'$ref': 'a'}
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'properties': {'$ref': {'$ref': '#/$defs/is-string'}}, '$defs': {'is-string': {'type': 'string'}}}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "propertynamedrefcontaininganactualref"
        
def test_property_named_ref_invalid(db_conn):
    data = {'$ref': 2}
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'properties': {'$ref': {'$ref': '#/$defs/is-string'}}, '$defs': {'is-string': {'type': 'string'}}}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "propertynamedrefcontaininganactualref"
        
def test_any_value_is_valid(db_conn):
    data = 'foo'
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', '$ref': '#/$defs/bool', '$defs': {'bool': True}}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "reftobooleanschematrue"
        
def test_any_value_is_invalid(db_conn):
    data = 'foo'
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', '$ref': '#/$defs/bool', '$defs': {'bool': False}}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "reftobooleanschemafalse"
        
def test_valid_tree(db_conn):
    data = {'meta': 'root', 'nodes': [{'value': 1, 'subtree': {'meta': 'child', 'nodes': [{'value': 1.1}, {'value': 1.2}]}}, {'value': 2, 'subtree': {'meta': 'child', 'nodes': [{'value': 2.1}, {'value': 2.2}]}}]}
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', '$id': 'http://localhost:1234/draft2020-12/tree', 'description': 'tree of nodes', 'type': 'object', 'properties': {'meta': {'type': 'string'}, 'nodes': {'type': 'array', 'items': {'$ref': 'node'}}}, 'required': ['meta', 'nodes'], '$defs': {'node': {'$id': 'http://localhost:1234/draft2020-12/node', 'description': 'node', 'type': 'object', 'properties': {'value': {'type': 'number'}, 'subtree': {'$ref': 'tree'}}, 'required': ['value']}}}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "Recursivereferencesbetweenschemas"
        
def test_invalid_tree(db_conn):
    data = {'meta': 'root', 'nodes': [{'value': 1, 'subtree': {'meta': 'child', 'nodes': [{'value': 'string is invalid'}, {'value': 1.2}]}}, {'value': 2, 'subtree': {'meta': 'child', 'nodes': [{'value': 2.1}, {'value': 2.2}]}}]}
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', '$id': 'http://localhost:1234/draft2020-12/tree', 'description': 'tree of nodes', 'type': 'object', 'properties': {'meta': {'type': 'string'}, 'nodes': {'type': 'array', 'items': {'$ref': 'node'}}}, 'required': ['meta', 'nodes'], '$defs': {'node': {'$id': 'http://localhost:1234/draft2020-12/node', 'description': 'node', 'type': 'object', 'properties': {'value': {'type': 'number'}, 'subtree': {'$ref': 'tree'}}, 'required': ['value']}}}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "Recursivereferencesbetweenschemas"
        
def test_object_with_numbers_is_valid(db_conn):
    data = {'foo"bar': 1}
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'properties': {'foo"bar': {'$ref': '#/$defs/foo%22bar'}}, '$defs': {'foo"bar': {'type': 'number'}}}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "refswithquote"
        
def test_object_with_strings_is_invalid(db_conn):
    data = {'foo"bar': '1'}
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'properties': {'foo"bar': {'$ref': '#/$defs/foo%22bar'}}, '$defs': {'foo"bar': {'type': 'number'}}}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "refswithquote"
        
def test_referenced_subschema_doesnt_see_annotations_from_properties(db_conn):
    data = {'prop1': 'match'}
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', '$defs': {'A': {'unevaluatedProperties': False}}, 'properties': {'prop1': {'type': 'string'}}, '$ref': '#/$defs/A'}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "refcreatesnewscopewhenadjacenttokeywords"
        
def test_do_not_evaluate_the_ref_inside_the_enum_matching_any_string(db_conn):
    data = 'this is a string'
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', '$defs': {'a_string': {'type': 'string'}}, 'enum': [{'$ref': '#/$defs/a_string'}]}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "naivereplacementofrefwithitsdestinationisnotcorrect"
        
def test_do_not_evaluate_the_ref_inside_the_enum_definition_exact_match(db_conn):
    data = {'type': 'string'}
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', '$defs': {'a_string': {'type': 'string'}}, 'enum': [{'$ref': '#/$defs/a_string'}]}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "naivereplacementofrefwithitsdestinationisnotcorrect"
        
def test_match_the_enum_exactly(db_conn):
    data = {'$ref': '#/$defs/a_string'}
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', '$defs': {'a_string': {'type': 'string'}}, 'enum': [{'$ref': '#/$defs/a_string'}]}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "naivereplacementofrefwithitsdestinationisnotcorrect"
        
def test_invalid_on_inner_field(db_conn):
    data = {'foo': {'bar': 1}, 'bar': 'a'}
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', '$id': 'http://example.com/schema-relative-uri-defs1.json', 'properties': {'foo': {'$id': 'schema-relative-uri-defs2.json', '$defs': {'inner': {'properties': {'bar': {'type': 'string'}}}}, '$ref': '#/$defs/inner'}}, '$ref': 'schema-relative-uri-defs2.json'}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "refswithrelativeurisanddefs"
        
def test_invalid_on_outer_field(db_conn):
    data = {'foo': {'bar': 'a'}, 'bar': 1}
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', '$id': 'http://example.com/schema-relative-uri-defs1.json', 'properties': {'foo': {'$id': 'schema-relative-uri-defs2.json', '$defs': {'inner': {'properties': {'bar': {'type': 'string'}}}}, '$ref': '#/$defs/inner'}}, '$ref': 'schema-relative-uri-defs2.json'}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "refswithrelativeurisanddefs"
        
def test_valid_on_both_fields(db_conn):
    data = {'foo': {'bar': 'a'}, 'bar': 'a'}
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', '$id': 'http://example.com/schema-relative-uri-defs1.json', 'properties': {'foo': {'$id': 'schema-relative-uri-defs2.json', '$defs': {'inner': {'properties': {'bar': {'type': 'string'}}}}, '$ref': '#/$defs/inner'}}, '$ref': 'schema-relative-uri-defs2.json'}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "refswithrelativeurisanddefs"
        
def test_invalid_on_inner_field(db_conn):
    data = {'foo': {'bar': 1}, 'bar': 'a'}
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', '$id': 'http://example.com/schema-refs-absolute-uris-defs1.json', 'properties': {'foo': {'$id': 'http://example.com/schema-refs-absolute-uris-defs2.json', '$defs': {'inner': {'properties': {'bar': {'type': 'string'}}}}, '$ref': '#/$defs/inner'}}, '$ref': 'schema-refs-absolute-uris-defs2.json'}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "relativerefswithabsoluteurisanddefs"
        
def test_invalid_on_outer_field(db_conn):
    data = {'foo': {'bar': 'a'}, 'bar': 1}
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', '$id': 'http://example.com/schema-refs-absolute-uris-defs1.json', 'properties': {'foo': {'$id': 'http://example.com/schema-refs-absolute-uris-defs2.json', '$defs': {'inner': {'properties': {'bar': {'type': 'string'}}}}, '$ref': '#/$defs/inner'}}, '$ref': 'schema-refs-absolute-uris-defs2.json'}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "relativerefswithabsoluteurisanddefs"
        
def test_valid_on_both_fields(db_conn):
    data = {'foo': {'bar': 'a'}, 'bar': 'a'}
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', '$id': 'http://example.com/schema-refs-absolute-uris-defs1.json', 'properties': {'foo': {'$id': 'http://example.com/schema-refs-absolute-uris-defs2.json', '$defs': {'inner': {'properties': {'bar': {'type': 'string'}}}}, '$ref': '#/$defs/inner'}}, '$ref': 'schema-refs-absolute-uris-defs2.json'}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "relativerefswithabsoluteurisanddefs"
        
def test_number_is_valid(db_conn):
    data = 1
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', '$id': 'http://example.com/a.json', '$defs': {'x': {'$id': 'http://example.com/b/c.json', 'not': {'$defs': {'y': {'$id': 'd.json', 'type': 'number'}}}}}, 'allOf': [{'$ref': 'http://example.com/b/d.json'}]}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "idmustberesolvedagainstnearestparentnotjustimmediateparent"
        
def test_nonnumber_is_invalid(db_conn):
    data = 'a'
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', '$id': 'http://example.com/a.json', '$defs': {'x': {'$id': 'http://example.com/b/c.json', 'not': {'$defs': {'y': {'$id': 'd.json', 'type': 'number'}}}}}, 'allOf': [{'$ref': 'http://example.com/b/d.json'}]}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "idmustberesolvedagainstnearestparentnotjustimmediateparent"
        
def test_data_is_valid_against_first_definition(db_conn):
    data = 5
    schema = {'$comment': '$id must be evaluated before $ref to get the proper $ref destination', '$schema': 'https://json-schema.org/draft/2020-12/schema', '$id': 'https://example.com/draft2020-12/ref-and-id1/base.json', '$ref': 'int.json', '$defs': {'bigint': {'$comment': 'canonical uri: https://example.com/ref-and-id1/int.json', '$id': 'int.json', 'maximum': 10}, 'smallint': {'$comment': 'canonical uri: https://example.com/ref-and-id1-int.json', '$id': '/draft2020-12/ref-and-id1-int.json', 'maximum': 2}}}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "orderofevaluationidandref"
        
def test_data_is_invalid_against_first_definition(db_conn):
    data = 50
    schema = {'$comment': '$id must be evaluated before $ref to get the proper $ref destination', '$schema': 'https://json-schema.org/draft/2020-12/schema', '$id': 'https://example.com/draft2020-12/ref-and-id1/base.json', '$ref': 'int.json', '$defs': {'bigint': {'$comment': 'canonical uri: https://example.com/ref-and-id1/int.json', '$id': 'int.json', 'maximum': 10}, 'smallint': {'$comment': 'canonical uri: https://example.com/ref-and-id1-int.json', '$id': '/draft2020-12/ref-and-id1-int.json', 'maximum': 2}}}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "orderofevaluationidandref"
        
def test_data_is_valid_against_first_definition(db_conn):
    data = 5
    schema = {'$comment': '$id must be evaluated before $ref to get the proper $ref destination', '$schema': 'https://json-schema.org/draft/2020-12/schema', '$id': 'https://example.com/draft2020-12/ref-and-id2/base.json', '$ref': '#bigint', '$defs': {'bigint': {'$comment': 'canonical uri: /ref-and-id2/base.json#/$defs/bigint; another valid uri for this location: /ref-and-id2/base.json#bigint', '$anchor': 'bigint', 'maximum': 10}, 'smallint': {'$comment': 'canonical uri: https://example.com/ref-and-id2#/$defs/smallint; another valid uri for this location: https://example.com/ref-and-id2/#bigint', '$id': 'https://example.com/draft2020-12/ref-and-id2/', '$anchor': 'bigint', 'maximum': 2}}}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "orderofevaluationidandanchorandref"
        
def test_data_is_invalid_against_first_definition(db_conn):
    data = 50
    schema = {'$comment': '$id must be evaluated before $ref to get the proper $ref destination', '$schema': 'https://json-schema.org/draft/2020-12/schema', '$id': 'https://example.com/draft2020-12/ref-and-id2/base.json', '$ref': '#bigint', '$defs': {'bigint': {'$comment': 'canonical uri: /ref-and-id2/base.json#/$defs/bigint; another valid uri for this location: /ref-and-id2/base.json#bigint', '$anchor': 'bigint', 'maximum': 10}, 'smallint': {'$comment': 'canonical uri: https://example.com/ref-and-id2#/$defs/smallint; another valid uri for this location: https://example.com/ref-and-id2/#bigint', '$id': 'https://example.com/draft2020-12/ref-and-id2/', '$anchor': 'bigint', 'maximum': 2}}}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "orderofevaluationidandanchorandref"
        
def test_valid_under_the_URN_IDed_schema(db_conn):
    data = {'foo': 37}
    schema = {'$comment': 'URIs do not have to have HTTP(s) schemes', '$schema': 'https://json-schema.org/draft/2020-12/schema', '$id': 'urn:uuid:deadbeef-1234-ffff-ffff-4321feebdaed', 'minimum': 30, 'properties': {'foo': {'$ref': 'urn:uuid:deadbeef-1234-ffff-ffff-4321feebdaed'}}}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "simpleURNbaseURIwithrefviatheURN"
        
def test_invalid_under_the_URN_IDed_schema(db_conn):
    data = {'foo': 12}
    schema = {'$comment': 'URIs do not have to have HTTP(s) schemes', '$schema': 'https://json-schema.org/draft/2020-12/schema', '$id': 'urn:uuid:deadbeef-1234-ffff-ffff-4321feebdaed', 'minimum': 30, 'properties': {'foo': {'$ref': 'urn:uuid:deadbeef-1234-ffff-ffff-4321feebdaed'}}}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "simpleURNbaseURIwithrefviatheURN"
        
def test_a_string_is_valid(db_conn):
    data = {'foo': 'bar'}
    schema = {'$comment': 'URIs do not have to have HTTP(s) schemes', '$schema': 'https://json-schema.org/draft/2020-12/schema', '$id': 'urn:uuid:deadbeef-1234-00ff-ff00-4321feebdaed', 'properties': {'foo': {'$ref': '#/$defs/bar'}}, '$defs': {'bar': {'type': 'string'}}}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "simpleURNbaseURIwithJSONpointer"
        
def test_a_nonstring_is_invalid(db_conn):
    data = {'foo': 12}
    schema = {'$comment': 'URIs do not have to have HTTP(s) schemes', '$schema': 'https://json-schema.org/draft/2020-12/schema', '$id': 'urn:uuid:deadbeef-1234-00ff-ff00-4321feebdaed', 'properties': {'foo': {'$ref': '#/$defs/bar'}}, '$defs': {'bar': {'type': 'string'}}}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "simpleURNbaseURIwithJSONpointer"
        
def test_a_string_is_valid(db_conn):
    data = {'foo': 'bar'}
    schema = {'$comment': 'RFC 8141 §2.2', '$schema': 'https://json-schema.org/draft/2020-12/schema', '$id': 'urn:example:1/406/47452/2', 'properties': {'foo': {'$ref': '#/$defs/bar'}}, '$defs': {'bar': {'type': 'string'}}}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "URNbaseURIwithNSS"
        
def test_a_nonstring_is_invalid(db_conn):
    data = {'foo': 12}
    schema = {'$comment': 'RFC 8141 §2.2', '$schema': 'https://json-schema.org/draft/2020-12/schema', '$id': 'urn:example:1/406/47452/2', 'properties': {'foo': {'$ref': '#/$defs/bar'}}, '$defs': {'bar': {'type': 'string'}}}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "URNbaseURIwithNSS"
        
def test_a_string_is_valid(db_conn):
    data = {'foo': 'bar'}
    schema = {'$comment': 'RFC 8141 §2.3.1', '$schema': 'https://json-schema.org/draft/2020-12/schema', '$id': 'urn:example:foo-bar-baz-qux?+CCResolve:cc=uk', 'properties': {'foo': {'$ref': '#/$defs/bar'}}, '$defs': {'bar': {'type': 'string'}}}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "URNbaseURIwithrcomponent"
        
def test_a_nonstring_is_invalid(db_conn):
    data = {'foo': 12}
    schema = {'$comment': 'RFC 8141 §2.3.1', '$schema': 'https://json-schema.org/draft/2020-12/schema', '$id': 'urn:example:foo-bar-baz-qux?+CCResolve:cc=uk', 'properties': {'foo': {'$ref': '#/$defs/bar'}}, '$defs': {'bar': {'type': 'string'}}}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "URNbaseURIwithrcomponent"
        
def test_a_string_is_valid(db_conn):
    data = {'foo': 'bar'}
    schema = {'$comment': 'RFC 8141 §2.3.2', '$schema': 'https://json-schema.org/draft/2020-12/schema', '$id': 'urn:example:weather?=op=map&lat=39.56&lon=-104.85&datetime=1969-07-21T02:56:15Z', 'properties': {'foo': {'$ref': '#/$defs/bar'}}, '$defs': {'bar': {'type': 'string'}}}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "URNbaseURIwithqcomponent"
        
def test_a_nonstring_is_invalid(db_conn):
    data = {'foo': 12}
    schema = {'$comment': 'RFC 8141 §2.3.2', '$schema': 'https://json-schema.org/draft/2020-12/schema', '$id': 'urn:example:weather?=op=map&lat=39.56&lon=-104.85&datetime=1969-07-21T02:56:15Z', 'properties': {'foo': {'$ref': '#/$defs/bar'}}, '$defs': {'bar': {'type': 'string'}}}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "URNbaseURIwithqcomponent"
        
def test_is_invalid(db_conn):
    data = {'$id': 'urn:example:foo-bar-baz-qux#somepart'}
    schema = {'$comment': "RFC 8141 §2.3.3, but we don't allow fragments", '$schema': 'https://json-schema.org/draft/2020-12/schema', '$ref': 'https://json-schema.org/draft/2020-12/schema'}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "URNbaseURIwithfcomponent"
        
def test_a_string_is_valid(db_conn):
    data = {'foo': 'bar'}
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', '$id': 'urn:uuid:deadbeef-1234-0000-0000-4321feebdaed', 'properties': {'foo': {'$ref': 'urn:uuid:deadbeef-1234-0000-0000-4321feebdaed#/$defs/bar'}}, '$defs': {'bar': {'type': 'string'}}}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "URNbaseURIwithURNandJSONpointerref"
        
def test_a_nonstring_is_invalid(db_conn):
    data = {'foo': 12}
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', '$id': 'urn:uuid:deadbeef-1234-0000-0000-4321feebdaed', 'properties': {'foo': {'$ref': 'urn:uuid:deadbeef-1234-0000-0000-4321feebdaed#/$defs/bar'}}, '$defs': {'bar': {'type': 'string'}}}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "URNbaseURIwithURNandJSONpointerref"
        
def test_a_string_is_valid(db_conn):
    data = {'foo': 'bar'}
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', '$id': 'urn:uuid:deadbeef-1234-ff00-00ff-4321feebdaed', 'properties': {'foo': {'$ref': 'urn:uuid:deadbeef-1234-ff00-00ff-4321feebdaed#something'}}, '$defs': {'bar': {'$anchor': 'something', 'type': 'string'}}}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "URNbaseURIwithURNandanchorref"
        
def test_a_nonstring_is_invalid(db_conn):
    data = {'foo': 12}
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', '$id': 'urn:uuid:deadbeef-1234-ff00-00ff-4321feebdaed', 'properties': {'foo': {'$ref': 'urn:uuid:deadbeef-1234-ff00-00ff-4321feebdaed#something'}}, '$defs': {'bar': {'$anchor': 'something', 'type': 'string'}}}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "URNbaseURIwithURNandanchorref"
        
def test_a_string_is_valid(db_conn):
    data = 'bar'
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', '$ref': 'urn:uuid:deadbeef-4321-ffff-ffff-1234feebdaed', '$defs': {'foo': {'$id': 'urn:uuid:deadbeef-4321-ffff-ffff-1234feebdaed', '$defs': {'bar': {'type': 'string'}}, '$ref': '#/$defs/bar'}}}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "URNrefwithnestedpointerref"
        
def test_a_nonstring_is_invalid(db_conn):
    data = 12
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', '$ref': 'urn:uuid:deadbeef-4321-ffff-ffff-1234feebdaed', '$defs': {'foo': {'$id': 'urn:uuid:deadbeef-4321-ffff-ffff-1234feebdaed', '$defs': {'bar': {'type': 'string'}}, '$ref': '#/$defs/bar'}}}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "URNrefwithnestedpointerref"
        
def test_a_noninteger_is_invalid_due_to_the_ref(db_conn):
    data = 'foo'
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', '$ref': 'http://example.com/ref/if', 'if': {'$id': 'http://example.com/ref/if', 'type': 'integer'}}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "reftoif"
        
def test_an_integer_is_valid(db_conn):
    data = 12
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', '$ref': 'http://example.com/ref/if', 'if': {'$id': 'http://example.com/ref/if', 'type': 'integer'}}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "reftoif"
        
def test_a_noninteger_is_invalid_due_to_the_ref(db_conn):
    data = 'foo'
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', '$ref': 'http://example.com/ref/then', 'then': {'$id': 'http://example.com/ref/then', 'type': 'integer'}}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "reftothen"
        
def test_an_integer_is_valid(db_conn):
    data = 12
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', '$ref': 'http://example.com/ref/then', 'then': {'$id': 'http://example.com/ref/then', 'type': 'integer'}}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "reftothen"
        
def test_a_noninteger_is_invalid_due_to_the_ref(db_conn):
    data = 'foo'
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', '$ref': 'http://example.com/ref/else', 'else': {'$id': 'http://example.com/ref/else', 'type': 'integer'}}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "reftoelse"
        
def test_an_integer_is_valid(db_conn):
    data = 12
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', '$ref': 'http://example.com/ref/else', 'else': {'$id': 'http://example.com/ref/else', 'type': 'integer'}}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "reftoelse"
        
def test_a_string_is_valid(db_conn):
    data = 'foo'
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', '$id': 'http://example.com/ref/absref.json', '$defs': {'a': {'$id': 'http://example.com/ref/absref/foobar.json', 'type': 'number'}, 'b': {'$id': 'http://example.com/absref/foobar.json', 'type': 'string'}}, '$ref': '/absref/foobar.json'}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "refwithabsolutepathreference"
        
def test_an_integer_is_invalid(db_conn):
    data = 12
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', '$id': 'http://example.com/ref/absref.json', '$defs': {'a': {'$id': 'http://example.com/ref/absref/foobar.json', 'type': 'number'}, 'b': {'$id': 'http://example.com/absref/foobar.json', 'type': 'string'}}, '$ref': '/absref/foobar.json'}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "refwithabsolutepathreference"
        
def test_number_is_valid(db_conn):
    data = 1
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', '$id': 'file:///folder/file.json', '$defs': {'foo': {'type': 'number'}}, '$ref': '#/$defs/foo'}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "idwithfileURIstillresolvespointersnix"
        
def test_nonnumber_is_invalid(db_conn):
    data = 'a'
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', '$id': 'file:///folder/file.json', '$defs': {'foo': {'type': 'number'}}, '$ref': '#/$defs/foo'}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "idwithfileURIstillresolvespointersnix"
        
def test_number_is_valid(db_conn):
    data = 1
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', '$id': 'file:///c:/folder/file.json', '$defs': {'foo': {'type': 'number'}}, '$ref': '#/$defs/foo'}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "idwithfileURIstillresolvespointerswindows"
        
def test_nonnumber_is_invalid(db_conn):
    data = 'a'
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', '$id': 'file:///c:/folder/file.json', '$defs': {'foo': {'type': 'number'}}, '$ref': '#/$defs/foo'}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "idwithfileURIstillresolvespointerswindows"
        
def test_number_is_valid(db_conn):
    data = 1
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', '$defs': {'': {'$defs': {'': {'type': 'number'}}}}, 'allOf': [{'$ref': '#/$defs//$defs/'}]}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "emptytokensinrefjsonpointer"
        
def test_nonnumber_is_invalid(db_conn):
    data = 'a'
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', '$defs': {'': {'$defs': {'': {'type': 'number'}}}}, 'allOf': [{'$ref': '#/$defs//$defs/'}]}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "emptytokensinrefjsonpointer"
        