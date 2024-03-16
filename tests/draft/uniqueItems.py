
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


def test_unique_array_of_integers_is_valid(db_conn):
    data = [1, 2]
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'uniqueItems': True}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "uniqueItemsvalidation"
        
def test_nonunique_array_of_integers_is_invalid(db_conn):
    data = [1, 1]
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'uniqueItems': True}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "uniqueItemsvalidation"
        
def test_nonunique_array_of_more_than_two_integers_is_invalid(db_conn):
    data = [1, 2, 1]
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'uniqueItems': True}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "uniqueItemsvalidation"
        
def test_numbers_are_unique_if_mathematically_unequal(db_conn):
    data = [1.0, 1.0, 1]
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'uniqueItems': True}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "uniqueItemsvalidation"
        
def test_false_is_not_equal_to_zero(db_conn):
    data = [0, False]
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'uniqueItems': True}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "uniqueItemsvalidation"
        
def test_true_is_not_equal_to_one(db_conn):
    data = [1, True]
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'uniqueItems': True}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "uniqueItemsvalidation"
        
def test_unique_array_of_strings_is_valid(db_conn):
    data = ['foo', 'bar', 'baz']
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'uniqueItems': True}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "uniqueItemsvalidation"
        
def test_nonunique_array_of_strings_is_invalid(db_conn):
    data = ['foo', 'bar', 'foo']
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'uniqueItems': True}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "uniqueItemsvalidation"
        
def test_unique_array_of_objects_is_valid(db_conn):
    data = [{'foo': 'bar'}, {'foo': 'baz'}]
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'uniqueItems': True}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "uniqueItemsvalidation"
        
def test_nonunique_array_of_objects_is_invalid(db_conn):
    data = [{'foo': 'bar'}, {'foo': 'bar'}]
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'uniqueItems': True}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "uniqueItemsvalidation"
        
def test_property_order_of_array_of_objects_is_ignored(db_conn):
    data = [{'foo': 'bar', 'bar': 'foo'}, {'bar': 'foo', 'foo': 'bar'}]
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'uniqueItems': True}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "uniqueItemsvalidation"
        
def test_unique_array_of_nested_objects_is_valid(db_conn):
    data = [{'foo': {'bar': {'baz': True}}}, {'foo': {'bar': {'baz': False}}}]
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'uniqueItems': True}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "uniqueItemsvalidation"
        
def test_nonunique_array_of_nested_objects_is_invalid(db_conn):
    data = [{'foo': {'bar': {'baz': True}}}, {'foo': {'bar': {'baz': True}}}]
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'uniqueItems': True}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "uniqueItemsvalidation"
        
def test_unique_array_of_arrays_is_valid(db_conn):
    data = [['foo'], ['bar']]
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'uniqueItems': True}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "uniqueItemsvalidation"
        
def test_nonunique_array_of_arrays_is_invalid(db_conn):
    data = [['foo'], ['foo']]
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'uniqueItems': True}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "uniqueItemsvalidation"
        
def test_nonunique_array_of_more_than_two_arrays_is_invalid(db_conn):
    data = [['foo'], ['bar'], ['foo']]
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'uniqueItems': True}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "uniqueItemsvalidation"
        
def test_1_and_true_are_unique(db_conn):
    data = [1, True]
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'uniqueItems': True}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "uniqueItemsvalidation"
        
def test_0_and_false_are_unique(db_conn):
    data = [0, False]
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'uniqueItems': True}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "uniqueItemsvalidation"
        
def test_1_and_true_are_unique(db_conn):
    data = [[1], [True]]
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'uniqueItems': True}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "uniqueItemsvalidation"
        
def test_0_and_false_are_unique(db_conn):
    data = [[0], [False]]
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'uniqueItems': True}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "uniqueItemsvalidation"
        
def test_nested_1_and_true_are_unique(db_conn):
    data = [[[1], 'foo'], [[True], 'foo']]
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'uniqueItems': True}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "uniqueItemsvalidation"
        
def test_nested_0_and_false_are_unique(db_conn):
    data = [[[0], 'foo'], [[False], 'foo']]
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'uniqueItems': True}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "uniqueItemsvalidation"
        
def test_unique_heterogeneous_types_are_valid(db_conn):
    data = [{}, [1], True, None, 1, '{}']
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'uniqueItems': True}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "uniqueItemsvalidation"
        
def test_nonunique_heterogeneous_types_are_invalid(db_conn):
    data = [{}, [1], True, None, {}, 1]
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'uniqueItems': True}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "uniqueItemsvalidation"
        
def test_different_objects_are_unique(db_conn):
    data = [{'a': 1, 'b': 2}, {'a': 2, 'b': 1}]
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'uniqueItems': True}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "uniqueItemsvalidation"
        
def test_objects_are_nonunique_despite_key_order(db_conn):
    data = [{'a': 1, 'b': 2}, {'b': 2, 'a': 1}]
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'uniqueItems': True}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "uniqueItemsvalidation"
        
def test_a_false_and_a_0_are_unique(db_conn):
    data = [{'a': False}, {'a': 0}]
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'uniqueItems': True}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "uniqueItemsvalidation"
        
def test_a_true_and_a_1_are_unique(db_conn):
    data = [{'a': True}, {'a': 1}]
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'uniqueItems': True}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "uniqueItemsvalidation"
        
def test_false_true_from_items_array_is_valid(db_conn):
    data = [False, True]
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'prefixItems': [{'type': 'boolean'}, {'type': 'boolean'}], 'uniqueItems': True}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "uniqueItemswithanarrayofitems"
        
def test_true_false_from_items_array_is_valid(db_conn):
    data = [True, False]
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'prefixItems': [{'type': 'boolean'}, {'type': 'boolean'}], 'uniqueItems': True}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "uniqueItemswithanarrayofitems"
        
def test_false_false_from_items_array_is_not_valid(db_conn):
    data = [False, False]
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'prefixItems': [{'type': 'boolean'}, {'type': 'boolean'}], 'uniqueItems': True}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "uniqueItemswithanarrayofitems"
        
def test_true_true_from_items_array_is_not_valid(db_conn):
    data = [True, True]
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'prefixItems': [{'type': 'boolean'}, {'type': 'boolean'}], 'uniqueItems': True}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "uniqueItemswithanarrayofitems"
        
def test_unique_array_extended_from_false_true_is_valid(db_conn):
    data = [False, True, 'foo', 'bar']
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'prefixItems': [{'type': 'boolean'}, {'type': 'boolean'}], 'uniqueItems': True}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "uniqueItemswithanarrayofitems"
        
def test_unique_array_extended_from_true_false_is_valid(db_conn):
    data = [True, False, 'foo', 'bar']
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'prefixItems': [{'type': 'boolean'}, {'type': 'boolean'}], 'uniqueItems': True}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "uniqueItemswithanarrayofitems"
        
def test_nonunique_array_extended_from_false_true_is_not_valid(db_conn):
    data = [False, True, 'foo', 'foo']
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'prefixItems': [{'type': 'boolean'}, {'type': 'boolean'}], 'uniqueItems': True}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "uniqueItemswithanarrayofitems"
        
def test_nonunique_array_extended_from_true_false_is_not_valid(db_conn):
    data = [True, False, 'foo', 'foo']
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'prefixItems': [{'type': 'boolean'}, {'type': 'boolean'}], 'uniqueItems': True}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "uniqueItemswithanarrayofitems"
        
def test_false_true_from_items_array_is_valid(db_conn):
    data = [False, True]
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'prefixItems': [{'type': 'boolean'}, {'type': 'boolean'}], 'uniqueItems': True, 'items': False}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "uniqueItemswithanarrayofitemsandadditionalItemsfalse"
        
def test_true_false_from_items_array_is_valid(db_conn):
    data = [True, False]
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'prefixItems': [{'type': 'boolean'}, {'type': 'boolean'}], 'uniqueItems': True, 'items': False}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "uniqueItemswithanarrayofitemsandadditionalItemsfalse"
        
def test_false_false_from_items_array_is_not_valid(db_conn):
    data = [False, False]
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'prefixItems': [{'type': 'boolean'}, {'type': 'boolean'}], 'uniqueItems': True, 'items': False}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "uniqueItemswithanarrayofitemsandadditionalItemsfalse"
        
def test_true_true_from_items_array_is_not_valid(db_conn):
    data = [True, True]
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'prefixItems': [{'type': 'boolean'}, {'type': 'boolean'}], 'uniqueItems': True, 'items': False}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "uniqueItemswithanarrayofitemsandadditionalItemsfalse"
        
def test_extra_items_are_invalid_even_if_unique(db_conn):
    data = [False, True, None]
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'prefixItems': [{'type': 'boolean'}, {'type': 'boolean'}], 'uniqueItems': True, 'items': False}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "uniqueItemswithanarrayofitemsandadditionalItemsfalse"
        
def test_unique_array_of_integers_is_valid(db_conn):
    data = [1, 2]
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'uniqueItems': False}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "uniqueItemsfalsevalidation"
        
def test_nonunique_array_of_integers_is_valid(db_conn):
    data = [1, 1]
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'uniqueItems': False}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "uniqueItemsfalsevalidation"
        
def test_numbers_are_unique_if_mathematically_unequal(db_conn):
    data = [1.0, 1.0, 1]
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'uniqueItems': False}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "uniqueItemsfalsevalidation"
        
def test_false_is_not_equal_to_zero(db_conn):
    data = [0, False]
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'uniqueItems': False}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "uniqueItemsfalsevalidation"
        
def test_true_is_not_equal_to_one(db_conn):
    data = [1, True]
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'uniqueItems': False}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "uniqueItemsfalsevalidation"
        
def test_unique_array_of_objects_is_valid(db_conn):
    data = [{'foo': 'bar'}, {'foo': 'baz'}]
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'uniqueItems': False}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "uniqueItemsfalsevalidation"
        
def test_nonunique_array_of_objects_is_valid(db_conn):
    data = [{'foo': 'bar'}, {'foo': 'bar'}]
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'uniqueItems': False}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "uniqueItemsfalsevalidation"
        
def test_unique_array_of_nested_objects_is_valid(db_conn):
    data = [{'foo': {'bar': {'baz': True}}}, {'foo': {'bar': {'baz': False}}}]
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'uniqueItems': False}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "uniqueItemsfalsevalidation"
        
def test_nonunique_array_of_nested_objects_is_valid(db_conn):
    data = [{'foo': {'bar': {'baz': True}}}, {'foo': {'bar': {'baz': True}}}]
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'uniqueItems': False}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "uniqueItemsfalsevalidation"
        
def test_unique_array_of_arrays_is_valid(db_conn):
    data = [['foo'], ['bar']]
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'uniqueItems': False}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "uniqueItemsfalsevalidation"
        
def test_nonunique_array_of_arrays_is_valid(db_conn):
    data = [['foo'], ['foo']]
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'uniqueItems': False}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "uniqueItemsfalsevalidation"
        
def test_1_and_true_are_unique(db_conn):
    data = [1, True]
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'uniqueItems': False}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "uniqueItemsfalsevalidation"
        
def test_0_and_false_are_unique(db_conn):
    data = [0, False]
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'uniqueItems': False}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "uniqueItemsfalsevalidation"
        
def test_unique_heterogeneous_types_are_valid(db_conn):
    data = [{}, [1], True, None, 1]
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'uniqueItems': False}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "uniqueItemsfalsevalidation"
        
def test_nonunique_heterogeneous_types_are_valid(db_conn):
    data = [{}, [1], True, None, {}, 1]
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'uniqueItems': False}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "uniqueItemsfalsevalidation"
        
def test_false_true_from_items_array_is_valid(db_conn):
    data = [False, True]
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'prefixItems': [{'type': 'boolean'}, {'type': 'boolean'}], 'uniqueItems': False}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "uniqueItemsfalsewithanarrayofitems"
        
def test_true_false_from_items_array_is_valid(db_conn):
    data = [True, False]
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'prefixItems': [{'type': 'boolean'}, {'type': 'boolean'}], 'uniqueItems': False}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "uniqueItemsfalsewithanarrayofitems"
        
def test_false_false_from_items_array_is_valid(db_conn):
    data = [False, False]
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'prefixItems': [{'type': 'boolean'}, {'type': 'boolean'}], 'uniqueItems': False}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "uniqueItemsfalsewithanarrayofitems"
        
def test_true_true_from_items_array_is_valid(db_conn):
    data = [True, True]
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'prefixItems': [{'type': 'boolean'}, {'type': 'boolean'}], 'uniqueItems': False}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "uniqueItemsfalsewithanarrayofitems"
        
def test_unique_array_extended_from_false_true_is_valid(db_conn):
    data = [False, True, 'foo', 'bar']
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'prefixItems': [{'type': 'boolean'}, {'type': 'boolean'}], 'uniqueItems': False}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "uniqueItemsfalsewithanarrayofitems"
        
def test_unique_array_extended_from_true_false_is_valid(db_conn):
    data = [True, False, 'foo', 'bar']
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'prefixItems': [{'type': 'boolean'}, {'type': 'boolean'}], 'uniqueItems': False}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "uniqueItemsfalsewithanarrayofitems"
        
def test_nonunique_array_extended_from_false_true_is_valid(db_conn):
    data = [False, True, 'foo', 'foo']
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'prefixItems': [{'type': 'boolean'}, {'type': 'boolean'}], 'uniqueItems': False}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "uniqueItemsfalsewithanarrayofitems"
        
def test_nonunique_array_extended_from_true_false_is_valid(db_conn):
    data = [True, False, 'foo', 'foo']
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'prefixItems': [{'type': 'boolean'}, {'type': 'boolean'}], 'uniqueItems': False}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "uniqueItemsfalsewithanarrayofitems"
        
def test_false_true_from_items_array_is_valid(db_conn):
    data = [False, True]
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'prefixItems': [{'type': 'boolean'}, {'type': 'boolean'}], 'uniqueItems': False, 'items': False}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "uniqueItemsfalsewithanarrayofitemsandadditionalItemsfalse"
        
def test_true_false_from_items_array_is_valid(db_conn):
    data = [True, False]
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'prefixItems': [{'type': 'boolean'}, {'type': 'boolean'}], 'uniqueItems': False, 'items': False}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "uniqueItemsfalsewithanarrayofitemsandadditionalItemsfalse"
        
def test_false_false_from_items_array_is_valid(db_conn):
    data = [False, False]
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'prefixItems': [{'type': 'boolean'}, {'type': 'boolean'}], 'uniqueItems': False, 'items': False}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "uniqueItemsfalsewithanarrayofitemsandadditionalItemsfalse"
        
def test_true_true_from_items_array_is_valid(db_conn):
    data = [True, True]
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'prefixItems': [{'type': 'boolean'}, {'type': 'boolean'}], 'uniqueItems': False, 'items': False}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "uniqueItemsfalsewithanarrayofitemsandadditionalItemsfalse"
        
def test_extra_items_are_invalid_even_if_unique(db_conn):
    data = [False, True, None]
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'prefixItems': [{'type': 'boolean'}, {'type': 'boolean'}], 'uniqueItems': False, 'items': False}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "uniqueItemsfalsewithanarrayofitemsandadditionalItemsfalse"
        