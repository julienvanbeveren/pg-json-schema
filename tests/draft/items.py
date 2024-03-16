
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


def test_valid_items(db_conn):
    data = [1, 2, 3]
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'items': {'type': 'integer'}}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "aschemagivenforitems"
        
def test_wrong_type_of_items(db_conn):
    data = [1, 'x']
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'items': {'type': 'integer'}}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "aschemagivenforitems"
        
def test_ignores_nonarrays(db_conn):
    data = {'foo': 'bar'}
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'items': {'type': 'integer'}}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "aschemagivenforitems"
        
def test_JavaScript_pseudoarray_is_valid(db_conn):
    data = {'0': 'invalid', 'length': 1}
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'items': {'type': 'integer'}}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "aschemagivenforitems"
        
def test_any_array_is_valid(db_conn):
    data = [1, 'foo', True]
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'items': True}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "itemswithbooleanschematrue"
        
def test_empty_array_is_valid(db_conn):
    data = []
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'items': True}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "itemswithbooleanschematrue"
        
def test_any_nonempty_array_is_invalid(db_conn):
    data = [1, 'foo', True]
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'items': False}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "itemswithbooleanschemafalse"
        
def test_empty_array_is_valid(db_conn):
    data = []
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'items': False}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "itemswithbooleanschemafalse"
        
def test_valid_items(db_conn):
    data = [[{'foo': None}, {'foo': None}], [{'foo': None}, {'foo': None}], [{'foo': None}, {'foo': None}]]
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', '$defs': {'item': {'type': 'array', 'items': False, 'prefixItems': [{'$ref': '#/$defs/sub-item'}, {'$ref': '#/$defs/sub-item'}]}, 'sub-item': {'type': 'object', 'required': ['foo']}}, 'type': 'array', 'items': False, 'prefixItems': [{'$ref': '#/$defs/item'}, {'$ref': '#/$defs/item'}, {'$ref': '#/$defs/item'}]}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "itemsandsubitems"
        
def test_too_many_items(db_conn):
    data = [[{'foo': None}, {'foo': None}], [{'foo': None}, {'foo': None}], [{'foo': None}, {'foo': None}], [{'foo': None}, {'foo': None}]]
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', '$defs': {'item': {'type': 'array', 'items': False, 'prefixItems': [{'$ref': '#/$defs/sub-item'}, {'$ref': '#/$defs/sub-item'}]}, 'sub-item': {'type': 'object', 'required': ['foo']}}, 'type': 'array', 'items': False, 'prefixItems': [{'$ref': '#/$defs/item'}, {'$ref': '#/$defs/item'}, {'$ref': '#/$defs/item'}]}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "itemsandsubitems"
        
def test_too_many_subitems(db_conn):
    data = [[{'foo': None}, {'foo': None}, {'foo': None}], [{'foo': None}, {'foo': None}], [{'foo': None}, {'foo': None}]]
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', '$defs': {'item': {'type': 'array', 'items': False, 'prefixItems': [{'$ref': '#/$defs/sub-item'}, {'$ref': '#/$defs/sub-item'}]}, 'sub-item': {'type': 'object', 'required': ['foo']}}, 'type': 'array', 'items': False, 'prefixItems': [{'$ref': '#/$defs/item'}, {'$ref': '#/$defs/item'}, {'$ref': '#/$defs/item'}]}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "itemsandsubitems"
        
def test_wrong_item(db_conn):
    data = [{'foo': None}, [{'foo': None}, {'foo': None}], [{'foo': None}, {'foo': None}]]
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', '$defs': {'item': {'type': 'array', 'items': False, 'prefixItems': [{'$ref': '#/$defs/sub-item'}, {'$ref': '#/$defs/sub-item'}]}, 'sub-item': {'type': 'object', 'required': ['foo']}}, 'type': 'array', 'items': False, 'prefixItems': [{'$ref': '#/$defs/item'}, {'$ref': '#/$defs/item'}, {'$ref': '#/$defs/item'}]}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "itemsandsubitems"
        
def test_wrong_subitem(db_conn):
    data = [[{}, {'foo': None}], [{'foo': None}, {'foo': None}], [{'foo': None}, {'foo': None}]]
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', '$defs': {'item': {'type': 'array', 'items': False, 'prefixItems': [{'$ref': '#/$defs/sub-item'}, {'$ref': '#/$defs/sub-item'}]}, 'sub-item': {'type': 'object', 'required': ['foo']}}, 'type': 'array', 'items': False, 'prefixItems': [{'$ref': '#/$defs/item'}, {'$ref': '#/$defs/item'}, {'$ref': '#/$defs/item'}]}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "itemsandsubitems"
        
def test_fewer_items_is_valid(db_conn):
    data = [[{'foo': None}], [{'foo': None}]]
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', '$defs': {'item': {'type': 'array', 'items': False, 'prefixItems': [{'$ref': '#/$defs/sub-item'}, {'$ref': '#/$defs/sub-item'}]}, 'sub-item': {'type': 'object', 'required': ['foo']}}, 'type': 'array', 'items': False, 'prefixItems': [{'$ref': '#/$defs/item'}, {'$ref': '#/$defs/item'}, {'$ref': '#/$defs/item'}]}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "itemsandsubitems"
        
def test_valid_nested_array(db_conn):
    data = [[[[1]], [[2], [3]]], [[[4], [5], [6]]]]
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'type': 'array', 'items': {'type': 'array', 'items': {'type': 'array', 'items': {'type': 'array', 'items': {'type': 'number'}}}}}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "nesteditems"
        
def test_nested_array_with_invalid_type(db_conn):
    data = [[[['1']], [[2], [3]]], [[[4], [5], [6]]]]
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'type': 'array', 'items': {'type': 'array', 'items': {'type': 'array', 'items': {'type': 'array', 'items': {'type': 'number'}}}}}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "nesteditems"
        
def test_not_deep_enough(db_conn):
    data = [[[1], [2], [3]], [[4], [5], [6]]]
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'type': 'array', 'items': {'type': 'array', 'items': {'type': 'array', 'items': {'type': 'array', 'items': {'type': 'number'}}}}}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "nesteditems"
        
def test_empty_array(db_conn):
    data = []
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'prefixItems': [{}, {}, {}], 'items': False}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "prefixItemswithnoadditionalitemsallowed"
        
def test_fewer_number_of_items_present_1(db_conn):
    data = [1]
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'prefixItems': [{}, {}, {}], 'items': False}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "prefixItemswithnoadditionalitemsallowed"
        
def test_fewer_number_of_items_present_2(db_conn):
    data = [1, 2]
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'prefixItems': [{}, {}, {}], 'items': False}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "prefixItemswithnoadditionalitemsallowed"
        
def test_equal_number_of_items_present(db_conn):
    data = [1, 2, 3]
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'prefixItems': [{}, {}, {}], 'items': False}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "prefixItemswithnoadditionalitemsallowed"
        
def test_additional_items_are_not_permitted(db_conn):
    data = [1, 2, 3, 4]
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'prefixItems': [{}, {}, {}], 'items': False}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "prefixItemswithnoadditionalitemsallowed"
        
def test_prefixItems_in_allOf_does_not_constrain_items_invalid_case(db_conn):
    data = [3, 5]
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'allOf': [{'prefixItems': [{'minimum': 3}]}], 'items': {'minimum': 5}}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "itemsdoesnotlookinapplicatorsvalidcase"
        
def test_prefixItems_in_allOf_does_not_constrain_items_valid_case(db_conn):
    data = [5, 5]
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'allOf': [{'prefixItems': [{'minimum': 3}]}], 'items': {'minimum': 5}}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "itemsdoesnotlookinapplicatorsvalidcase"
        
def test_valid_items(db_conn):
    data = ['x', 2, 3]
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'prefixItems': [{'type': 'string'}], 'items': {'type': 'integer'}}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "prefixItemsvalidationadjuststhestartingindexforitems"
        
def test_wrong_type_of_second_item(db_conn):
    data = ['x', 'y']
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'prefixItems': [{'type': 'string'}], 'items': {'type': 'integer'}}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "prefixItemsvalidationadjuststhestartingindexforitems"
        
def test_heterogeneous_invalid_instance(db_conn):
    data = ['foo', 'bar', 37]
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'prefixItems': [{}], 'items': False}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "itemswithheterogeneousarray"
        
def test_valid_instance(db_conn):
    data = [None]
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'prefixItems': [{}], 'items': False}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "itemswithheterogeneousarray"
        
def test_allows_null_elements(db_conn):
    data = [None]
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'items': {'type': 'null'}}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "itemswithnullinstanceelements"
        