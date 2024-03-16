
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


def test_with_no_unevaluated_items(db_conn):
    data = []
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'unevaluatedItems': True}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "unevaluatedItemstrue"
        
def test_with_unevaluated_items(db_conn):
    data = ['foo']
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'unevaluatedItems': True}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "unevaluatedItemstrue"
        
def test_with_no_unevaluated_items(db_conn):
    data = []
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'unevaluatedItems': False}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "unevaluatedItemsfalse"
        
def test_with_unevaluated_items(db_conn):
    data = ['foo']
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'unevaluatedItems': False}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "unevaluatedItemsfalse"
        
def test_with_no_unevaluated_items(db_conn):
    data = []
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'unevaluatedItems': {'type': 'string'}}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "unevaluatedItemsasschema"
        
def test_with_valid_unevaluated_items(db_conn):
    data = ['foo']
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'unevaluatedItems': {'type': 'string'}}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "unevaluatedItemsasschema"
        
def test_with_invalid_unevaluated_items(db_conn):
    data = [42]
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'unevaluatedItems': {'type': 'string'}}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "unevaluatedItemsasschema"
        
def test_unevaluatedItems_doesnt_apply(db_conn):
    data = ['foo', 'bar']
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'items': {'type': 'string'}, 'unevaluatedItems': False}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "unevaluatedItemswithuniformitems"
        
def test_with_no_unevaluated_items(db_conn):
    data = ['foo']
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'prefixItems': [{'type': 'string'}], 'unevaluatedItems': False}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "unevaluatedItemswithtuple"
        
def test_with_unevaluated_items(db_conn):
    data = ['foo', 'bar']
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'prefixItems': [{'type': 'string'}], 'unevaluatedItems': False}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "unevaluatedItemswithtuple"
        
def test_unevaluatedItems_doesnt_apply(db_conn):
    data = ['foo', 42]
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'prefixItems': [{'type': 'string'}], 'items': True, 'unevaluatedItems': False}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "unevaluatedItemswithitemsandprefixItems"
        
def test_valid_under_items(db_conn):
    data = [5, 6, 7, 8]
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'items': {'type': 'number'}, 'unevaluatedItems': {'type': 'string'}}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "unevaluatedItemswithitems"
        
def test_invalid_under_items(db_conn):
    data = ['foo', 'bar', 'baz']
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'items': {'type': 'number'}, 'unevaluatedItems': {'type': 'string'}}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "unevaluatedItemswithitems"
        
def test_with_no_unevaluated_items(db_conn):
    data = ['foo', 42]
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'prefixItems': [{'type': 'string'}], 'allOf': [{'prefixItems': [True, {'type': 'number'}]}], 'unevaluatedItems': False}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "unevaluatedItemswithnestedtuple"
        
def test_with_unevaluated_items(db_conn):
    data = ['foo', 42, True]
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'prefixItems': [{'type': 'string'}], 'allOf': [{'prefixItems': [True, {'type': 'number'}]}], 'unevaluatedItems': False}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "unevaluatedItemswithnestedtuple"
        
def test_with_only_valid_additional_items(db_conn):
    data = [True, False]
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'unevaluatedItems': {'type': 'boolean'}, 'anyOf': [{'items': {'type': 'string'}}, True]}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "unevaluatedItemswithnesteditems"
        
def test_with_no_additional_items(db_conn):
    data = ['yes', 'no']
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'unevaluatedItems': {'type': 'boolean'}, 'anyOf': [{'items': {'type': 'string'}}, True]}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "unevaluatedItemswithnesteditems"
        
def test_with_invalid_additional_item(db_conn):
    data = ['yes', False]
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'unevaluatedItems': {'type': 'boolean'}, 'anyOf': [{'items': {'type': 'string'}}, True]}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "unevaluatedItemswithnesteditems"
        
def test_with_no_additional_items(db_conn):
    data = ['foo']
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'allOf': [{'prefixItems': [{'type': 'string'}], 'items': True}], 'unevaluatedItems': False}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "unevaluatedItemswithnestedprefixItemsanditems"
        
def test_with_additional_items(db_conn):
    data = ['foo', 42, True]
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'allOf': [{'prefixItems': [{'type': 'string'}], 'items': True}], 'unevaluatedItems': False}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "unevaluatedItemswithnestedprefixItemsanditems"
        
def test_with_no_additional_items(db_conn):
    data = ['foo']
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'allOf': [{'prefixItems': [{'type': 'string'}]}, {'unevaluatedItems': True}], 'unevaluatedItems': False}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "unevaluatedItemswithnestedunevaluatedItems"
        
def test_with_additional_items(db_conn):
    data = ['foo', 42, True]
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'allOf': [{'prefixItems': [{'type': 'string'}]}, {'unevaluatedItems': True}], 'unevaluatedItems': False}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "unevaluatedItemswithnestedunevaluatedItems"
        
def test_when_one_schema_matches_and_has_no_unevaluated_items(db_conn):
    data = ['foo', 'bar']
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'prefixItems': [{'const': 'foo'}], 'anyOf': [{'prefixItems': [True, {'const': 'bar'}]}, {'prefixItems': [True, True, {'const': 'baz'}]}], 'unevaluatedItems': False}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "unevaluatedItemswithanyOf"
        
def test_when_one_schema_matches_and_has_unevaluated_items(db_conn):
    data = ['foo', 'bar', 42]
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'prefixItems': [{'const': 'foo'}], 'anyOf': [{'prefixItems': [True, {'const': 'bar'}]}, {'prefixItems': [True, True, {'const': 'baz'}]}], 'unevaluatedItems': False}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "unevaluatedItemswithanyOf"
        
def test_when_two_schemas_match_and_has_no_unevaluated_items(db_conn):
    data = ['foo', 'bar', 'baz']
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'prefixItems': [{'const': 'foo'}], 'anyOf': [{'prefixItems': [True, {'const': 'bar'}]}, {'prefixItems': [True, True, {'const': 'baz'}]}], 'unevaluatedItems': False}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "unevaluatedItemswithanyOf"
        
def test_when_two_schemas_match_and_has_unevaluated_items(db_conn):
    data = ['foo', 'bar', 'baz', 42]
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'prefixItems': [{'const': 'foo'}], 'anyOf': [{'prefixItems': [True, {'const': 'bar'}]}, {'prefixItems': [True, True, {'const': 'baz'}]}], 'unevaluatedItems': False}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "unevaluatedItemswithanyOf"
        
def test_with_no_unevaluated_items(db_conn):
    data = ['foo', 'bar']
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'prefixItems': [{'const': 'foo'}], 'oneOf': [{'prefixItems': [True, {'const': 'bar'}]}, {'prefixItems': [True, {'const': 'baz'}]}], 'unevaluatedItems': False}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "unevaluatedItemswithoneOf"
        
def test_with_unevaluated_items(db_conn):
    data = ['foo', 'bar', 42]
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'prefixItems': [{'const': 'foo'}], 'oneOf': [{'prefixItems': [True, {'const': 'bar'}]}, {'prefixItems': [True, {'const': 'baz'}]}], 'unevaluatedItems': False}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "unevaluatedItemswithoneOf"
        
def test_with_unevaluated_items(db_conn):
    data = ['foo', 'bar']
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'prefixItems': [{'const': 'foo'}], 'not': {'not': {'prefixItems': [True, {'const': 'bar'}]}}, 'unevaluatedItems': False}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "unevaluatedItemswithnot"
        
def test_when_if_matches_and_it_has_no_unevaluated_items(db_conn):
    data = ['foo', 'bar', 'then']
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'prefixItems': [{'const': 'foo'}], 'if': {'prefixItems': [True, {'const': 'bar'}]}, 'then': {'prefixItems': [True, True, {'const': 'then'}]}, 'else': {'prefixItems': [True, True, True, {'const': 'else'}]}, 'unevaluatedItems': False}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "unevaluatedItemswithifthenelse"
        
def test_when_if_matches_and_it_has_unevaluated_items(db_conn):
    data = ['foo', 'bar', 'then', 'else']
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'prefixItems': [{'const': 'foo'}], 'if': {'prefixItems': [True, {'const': 'bar'}]}, 'then': {'prefixItems': [True, True, {'const': 'then'}]}, 'else': {'prefixItems': [True, True, True, {'const': 'else'}]}, 'unevaluatedItems': False}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "unevaluatedItemswithifthenelse"
        
def test_when_if_doesnt_match_and_it_has_no_unevaluated_items(db_conn):
    data = ['foo', 42, 42, 'else']
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'prefixItems': [{'const': 'foo'}], 'if': {'prefixItems': [True, {'const': 'bar'}]}, 'then': {'prefixItems': [True, True, {'const': 'then'}]}, 'else': {'prefixItems': [True, True, True, {'const': 'else'}]}, 'unevaluatedItems': False}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "unevaluatedItemswithifthenelse"
        
def test_when_if_doesnt_match_and_it_has_unevaluated_items(db_conn):
    data = ['foo', 42, 42, 'else', 42]
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'prefixItems': [{'const': 'foo'}], 'if': {'prefixItems': [True, {'const': 'bar'}]}, 'then': {'prefixItems': [True, True, {'const': 'then'}]}, 'else': {'prefixItems': [True, True, True, {'const': 'else'}]}, 'unevaluatedItems': False}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "unevaluatedItemswithifthenelse"
        
def test_with_no_unevaluated_items(db_conn):
    data = []
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'allOf': [True], 'unevaluatedItems': False}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "unevaluatedItemswithbooleanschemas"
        
def test_with_unevaluated_items(db_conn):
    data = ['foo']
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'allOf': [True], 'unevaluatedItems': False}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "unevaluatedItemswithbooleanschemas"
        
def test_with_no_unevaluated_items(db_conn):
    data = ['foo', 'bar']
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', '$ref': '#/$defs/bar', 'prefixItems': [{'type': 'string'}], 'unevaluatedItems': False, '$defs': {'bar': {'prefixItems': [True, {'type': 'string'}]}}}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "unevaluatedItemswithref"
        
def test_with_unevaluated_items(db_conn):
    data = ['foo', 'bar', 'baz']
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', '$ref': '#/$defs/bar', 'prefixItems': [{'type': 'string'}], 'unevaluatedItems': False, '$defs': {'bar': {'prefixItems': [True, {'type': 'string'}]}}}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "unevaluatedItemswithref"
        
def test_with_no_unevaluated_items(db_conn):
    data = ['foo', 'bar']
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'unevaluatedItems': False, 'prefixItems': [{'type': 'string'}], '$ref': '#/$defs/bar', '$defs': {'bar': {'prefixItems': [True, {'type': 'string'}]}}}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "unevaluatedItemsbeforeref"
        
def test_with_unevaluated_items(db_conn):
    data = ['foo', 'bar', 'baz']
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'unevaluatedItems': False, 'prefixItems': [{'type': 'string'}], '$ref': '#/$defs/bar', '$defs': {'bar': {'prefixItems': [True, {'type': 'string'}]}}}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "unevaluatedItemsbeforeref"
        
def test_with_no_unevaluated_items(db_conn):
    data = ['foo', 'bar']
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', '$id': 'https://example.com/unevaluated-items-with-dynamic-ref/derived', '$ref': './baseSchema', '$defs': {'derived': {'$dynamicAnchor': 'addons', 'prefixItems': [True, {'type': 'string'}]}, 'baseSchema': {'$id': './baseSchema', '$comment': "unevaluatedItems comes first so it's more likely to catch bugs with implementations that are sensitive to keyword ordering", 'unevaluatedItems': False, 'type': 'array', 'prefixItems': [{'type': 'string'}], '$dynamicRef': '#addons', '$defs': {'defaultAddons': {'$comment': 'Needed to satisfy the bookending requirement', '$dynamicAnchor': 'addons'}}}}}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "unevaluatedItemswithdynamicRef"
        
def test_with_unevaluated_items(db_conn):
    data = ['foo', 'bar', 'baz']
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', '$id': 'https://example.com/unevaluated-items-with-dynamic-ref/derived', '$ref': './baseSchema', '$defs': {'derived': {'$dynamicAnchor': 'addons', 'prefixItems': [True, {'type': 'string'}]}, 'baseSchema': {'$id': './baseSchema', '$comment': "unevaluatedItems comes first so it's more likely to catch bugs with implementations that are sensitive to keyword ordering", 'unevaluatedItems': False, 'type': 'array', 'prefixItems': [{'type': 'string'}], '$dynamicRef': '#addons', '$defs': {'defaultAddons': {'$comment': 'Needed to satisfy the bookending requirement', '$dynamicAnchor': 'addons'}}}}}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "unevaluatedItemswithdynamicRef"
        
def test_always_fails(db_conn):
    data = [1]
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'allOf': [{'prefixItems': [True]}, {'unevaluatedItems': False}]}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "unevaluatedItemscantseeinsidecousins"
        
def test_no_extra_items(db_conn):
    data = {'foo': ['test']}
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'properties': {'foo': {'prefixItems': [{'type': 'string'}], 'unevaluatedItems': False}}, 'anyOf': [{'properties': {'foo': {'prefixItems': [True, {'type': 'string'}]}}}]}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "itemisevaluatedinanuncleschematounevaluatedItems"
        
def test_uncle_keyword_evaluation_is_not_significant(db_conn):
    data = {'foo': ['test', 'test']}
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'properties': {'foo': {'prefixItems': [{'type': 'string'}], 'unevaluatedItems': False}}, 'anyOf': [{'properties': {'foo': {'prefixItems': [True, {'type': 'string'}]}}}]}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "itemisevaluatedinanuncleschematounevaluatedItems"
        
def test_second_item_is_evaluated_by_contains(db_conn):
    data = [1, 'foo']
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'prefixItems': [True], 'contains': {'type': 'string'}, 'unevaluatedItems': False}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "unevaluatedItemsdependsonadjacentcontains"
        
def test_contains_fails_second_item_is_not_evaluated(db_conn):
    data = [1, 2]
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'prefixItems': [True], 'contains': {'type': 'string'}, 'unevaluatedItems': False}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "unevaluatedItemsdependsonadjacentcontains"
        
def test_contains_passes_second_item_is_not_evaluated(db_conn):
    data = [1, 2, 'foo']
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'prefixItems': [True], 'contains': {'type': 'string'}, 'unevaluatedItems': False}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "unevaluatedItemsdependsonadjacentcontains"
        
def test_5_not_evaluated_passes_unevaluatedItems(db_conn):
    data = [2, 3, 4, 5, 6]
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'allOf': [{'contains': {'multipleOf': 2}}, {'contains': {'multipleOf': 3}}], 'unevaluatedItems': {'multipleOf': 5}}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "unevaluatedItemsdependsonmultiplenestedcontains"
        
def test_7_not_evaluated_fails_unevaluatedItems(db_conn):
    data = [2, 3, 4, 7, 8]
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'allOf': [{'contains': {'multipleOf': 2}}, {'contains': {'multipleOf': 3}}], 'unevaluatedItems': {'multipleOf': 5}}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "unevaluatedItemsdependsonmultiplenestedcontains"
        
def test_empty_array_is_valid(db_conn):
    data = []
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'if': {'contains': {'const': 'a'}}, 'then': {'if': {'contains': {'const': 'b'}}, 'then': {'if': {'contains': {'const': 'c'}}}}, 'unevaluatedItems': False}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "unevaluatedItemsandcontainsinteracttocontrolitemdependencyrelationship"
        
def test_only_as_are_valid(db_conn):
    data = ['a', 'a']
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'if': {'contains': {'const': 'a'}}, 'then': {'if': {'contains': {'const': 'b'}}, 'then': {'if': {'contains': {'const': 'c'}}}}, 'unevaluatedItems': False}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "unevaluatedItemsandcontainsinteracttocontrolitemdependencyrelationship"
        
def test_as_and_bs_are_valid(db_conn):
    data = ['a', 'b', 'a', 'b', 'a']
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'if': {'contains': {'const': 'a'}}, 'then': {'if': {'contains': {'const': 'b'}}, 'then': {'if': {'contains': {'const': 'c'}}}}, 'unevaluatedItems': False}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "unevaluatedItemsandcontainsinteracttocontrolitemdependencyrelationship"
        
def test_as_bs_and_cs_are_valid(db_conn):
    data = ['c', 'a', 'c', 'c', 'b', 'a']
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'if': {'contains': {'const': 'a'}}, 'then': {'if': {'contains': {'const': 'b'}}, 'then': {'if': {'contains': {'const': 'c'}}}}, 'unevaluatedItems': False}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "unevaluatedItemsandcontainsinteracttocontrolitemdependencyrelationship"
        
def test_only_bs_are_invalid(db_conn):
    data = ['b', 'b']
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'if': {'contains': {'const': 'a'}}, 'then': {'if': {'contains': {'const': 'b'}}, 'then': {'if': {'contains': {'const': 'c'}}}}, 'unevaluatedItems': False}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "unevaluatedItemsandcontainsinteracttocontrolitemdependencyrelationship"
        
def test_only_cs_are_invalid(db_conn):
    data = ['c', 'c']
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'if': {'contains': {'const': 'a'}}, 'then': {'if': {'contains': {'const': 'b'}}, 'then': {'if': {'contains': {'const': 'c'}}}}, 'unevaluatedItems': False}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "unevaluatedItemsandcontainsinteracttocontrolitemdependencyrelationship"
        
def test_only_bs_and_cs_are_invalid(db_conn):
    data = ['c', 'b', 'c', 'b', 'c']
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'if': {'contains': {'const': 'a'}}, 'then': {'if': {'contains': {'const': 'b'}}, 'then': {'if': {'contains': {'const': 'c'}}}}, 'unevaluatedItems': False}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "unevaluatedItemsandcontainsinteracttocontrolitemdependencyrelationship"
        
def test_only_as_and_cs_are_invalid(db_conn):
    data = ['c', 'a', 'c', 'a', 'c']
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'if': {'contains': {'const': 'a'}}, 'then': {'if': {'contains': {'const': 'b'}}, 'then': {'if': {'contains': {'const': 'c'}}}}, 'unevaluatedItems': False}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "unevaluatedItemsandcontainsinteracttocontrolitemdependencyrelationship"
        
def test_ignores_booleans(db_conn):
    data = True
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'unevaluatedItems': False}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "nonarrayinstancesarevalid"
        
def test_ignores_integers(db_conn):
    data = 123
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'unevaluatedItems': False}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "nonarrayinstancesarevalid"
        
def test_ignores_floats(db_conn):
    data = 1.0
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'unevaluatedItems': False}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "nonarrayinstancesarevalid"
        
def test_ignores_objects(db_conn):
    data = {}
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'unevaluatedItems': False}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "nonarrayinstancesarevalid"
        
def test_ignores_strings(db_conn):
    data = 'foo'
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'unevaluatedItems': False}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "nonarrayinstancesarevalid"
        
def test_ignores_null(db_conn):
    data = null
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'unevaluatedItems': False}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "nonarrayinstancesarevalid"
        
def test_allows_null_elements(db_conn):
    data = [None]
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'unevaluatedItems': {'type': 'null'}}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "unevaluatedItemswithnullinstanceelements"
        
def test_valid_in_case_if_is_evaluated(db_conn):
    data = ['a']
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'if': {'prefixItems': [{'const': 'a'}]}, 'unevaluatedItems': False}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "unevaluatedItemscanseeannotationsfromifwithoutthenandelse"
        
def test_invalid_in_case_if_is_evaluated(db_conn):
    data = ['b']
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'if': {'prefixItems': [{'const': 'a'}]}, 'unevaluatedItems': False}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "unevaluatedItemscanseeannotationsfromifwithoutthenandelse"
        