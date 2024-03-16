
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


def test_An_array_of_strings_is_valid(db_conn):
    data = ['foo', 'bar']
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', '$id': 'https://test.json-schema.org/dynamicRef-dynamicAnchor-same-schema/root', 'type': 'array', 'items': {'$dynamicRef': '#items'}, '$defs': {'foo': {'$dynamicAnchor': 'items', 'type': 'string'}}}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "AdynamicReftoadynamicAnchorinthesameschemaresourcebehaveslikeanormalreftoananchor"
        
def test_An_array_containing_nonstrings_is_invalid(db_conn):
    data = ['foo', 42]
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', '$id': 'https://test.json-schema.org/dynamicRef-dynamicAnchor-same-schema/root', 'type': 'array', 'items': {'$dynamicRef': '#items'}, '$defs': {'foo': {'$dynamicAnchor': 'items', 'type': 'string'}}}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "AdynamicReftoadynamicAnchorinthesameschemaresourcebehaveslikeanormalreftoananchor"
        
def test_An_array_of_strings_is_valid(db_conn):
    data = ['foo', 'bar']
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', '$id': 'https://test.json-schema.org/dynamicRef-anchor-same-schema/root', 'type': 'array', 'items': {'$dynamicRef': '#items'}, '$defs': {'foo': {'$anchor': 'items', 'type': 'string'}}}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "AdynamicReftoananchorinthesameschemaresourcebehaveslikeanormalreftoananchor"
        
def test_An_array_containing_nonstrings_is_invalid(db_conn):
    data = ['foo', 42]
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', '$id': 'https://test.json-schema.org/dynamicRef-anchor-same-schema/root', 'type': 'array', 'items': {'$dynamicRef': '#items'}, '$defs': {'foo': {'$anchor': 'items', 'type': 'string'}}}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "AdynamicReftoananchorinthesameschemaresourcebehaveslikeanormalreftoananchor"
        
def test_An_array_of_strings_is_valid(db_conn):
    data = ['foo', 'bar']
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', '$id': 'https://test.json-schema.org/ref-dynamicAnchor-same-schema/root', 'type': 'array', 'items': {'$ref': '#items'}, '$defs': {'foo': {'$dynamicAnchor': 'items', 'type': 'string'}}}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "AreftoadynamicAnchorinthesameschemaresourcebehaveslikeanormalreftoananchor"
        
def test_An_array_containing_nonstrings_is_invalid(db_conn):
    data = ['foo', 42]
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', '$id': 'https://test.json-schema.org/ref-dynamicAnchor-same-schema/root', 'type': 'array', 'items': {'$ref': '#items'}, '$defs': {'foo': {'$dynamicAnchor': 'items', 'type': 'string'}}}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "AreftoadynamicAnchorinthesameschemaresourcebehaveslikeanormalreftoananchor"
        
def test_An_array_of_strings_is_valid(db_conn):
    data = ['foo', 'bar']
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', '$id': 'https://test.json-schema.org/typical-dynamic-resolution/root', '$ref': 'list', '$defs': {'foo': {'$dynamicAnchor': 'items', 'type': 'string'}, 'list': {'$id': 'list', 'type': 'array', 'items': {'$dynamicRef': '#items'}, '$defs': {'items': {'$comment': 'This is only needed to satisfy the bookending requirement', '$dynamicAnchor': 'items'}}}}}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "AdynamicRefresolvestothefirstdynamicAnchorstillinscopethatisencounteredwhentheschemaisevaluated"
        
def test_An_array_containing_nonstrings_is_invalid(db_conn):
    data = ['foo', 42]
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', '$id': 'https://test.json-schema.org/typical-dynamic-resolution/root', '$ref': 'list', '$defs': {'foo': {'$dynamicAnchor': 'items', 'type': 'string'}, 'list': {'$id': 'list', 'type': 'array', 'items': {'$dynamicRef': '#items'}, '$defs': {'items': {'$comment': 'This is only needed to satisfy the bookending requirement', '$dynamicAnchor': 'items'}}}}}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "AdynamicRefresolvestothefirstdynamicAnchorstillinscopethatisencounteredwhentheschemaisevaluated"
        
def test_An_array_of_strings_is_invalid(db_conn):
    data = ['foo', 'bar']
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', '$id': 'https://test.json-schema.org/dynamicRef-without-anchor/root', '$ref': 'list', '$defs': {'foo': {'$dynamicAnchor': 'items', 'type': 'string'}, 'list': {'$id': 'list', 'type': 'array', 'items': {'$dynamicRef': '#/$defs/items'}, '$defs': {'items': {'$comment': 'This is only needed to satisfy the bookending requirement', '$dynamicAnchor': 'items', 'type': 'number'}}}}}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "AdynamicRefwithoutanchorinfragmentbehavesidenticaltoref"
        
def test_An_array_of_numbers_is_valid(db_conn):
    data = [24, 42]
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', '$id': 'https://test.json-schema.org/dynamicRef-without-anchor/root', '$ref': 'list', '$defs': {'foo': {'$dynamicAnchor': 'items', 'type': 'string'}, 'list': {'$id': 'list', 'type': 'array', 'items': {'$dynamicRef': '#/$defs/items'}, '$defs': {'items': {'$comment': 'This is only needed to satisfy the bookending requirement', '$dynamicAnchor': 'items', 'type': 'number'}}}}}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "AdynamicRefwithoutanchorinfragmentbehavesidenticaltoref"
        
def test_An_array_of_strings_is_valid(db_conn):
    data = ['foo', 'bar']
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', '$id': 'https://test.json-schema.org/dynamic-resolution-with-intermediate-scopes/root', '$ref': 'intermediate-scope', '$defs': {'foo': {'$dynamicAnchor': 'items', 'type': 'string'}, 'intermediate-scope': {'$id': 'intermediate-scope', '$ref': 'list'}, 'list': {'$id': 'list', 'type': 'array', 'items': {'$dynamicRef': '#items'}, '$defs': {'items': {'$comment': 'This is only needed to satisfy the bookending requirement', '$dynamicAnchor': 'items'}}}}}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "AdynamicRefwithintermediatescopesthatdontincludeamatchingdynamicAnchordoesnotaffectdynamicscoperesolution"
        
def test_An_array_containing_nonstrings_is_invalid(db_conn):
    data = ['foo', 42]
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', '$id': 'https://test.json-schema.org/dynamic-resolution-with-intermediate-scopes/root', '$ref': 'intermediate-scope', '$defs': {'foo': {'$dynamicAnchor': 'items', 'type': 'string'}, 'intermediate-scope': {'$id': 'intermediate-scope', '$ref': 'list'}, 'list': {'$id': 'list', 'type': 'array', 'items': {'$dynamicRef': '#items'}, '$defs': {'items': {'$comment': 'This is only needed to satisfy the bookending requirement', '$dynamicAnchor': 'items'}}}}}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "AdynamicRefwithintermediatescopesthatdontincludeamatchingdynamicAnchordoesnotaffectdynamicscoperesolution"
        
def test_Any_array_is_valid(db_conn):
    data = ['foo', 42]
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', '$id': 'https://test.json-schema.org/dynamic-resolution-ignores-anchors/root', '$ref': 'list', '$defs': {'foo': {'$anchor': 'items', 'type': 'string'}, 'list': {'$id': 'list', 'type': 'array', 'items': {'$dynamicRef': '#items'}, '$defs': {'items': {'$comment': 'This is only needed to satisfy the bookending requirement', '$dynamicAnchor': 'items'}}}}}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "AnanchorwiththesamenameasadynamicAnchorisnotusedfordynamicscoperesolution"
        
def test_Any_array_is_valid(db_conn):
    data = ['foo', 42]
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', '$id': 'https://test.json-schema.org/dynamic-resolution-without-bookend/root', '$ref': 'list', '$defs': {'foo': {'$dynamicAnchor': 'items', 'type': 'string'}, 'list': {'$id': 'list', 'type': 'array', 'items': {'$dynamicRef': '#items'}, '$defs': {'items': {'$comment': 'This is only needed to give the reference somewhere to resolve to when it behaves like $ref', '$anchor': 'items'}}}}}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "AdynamicRefwithoutamatchingdynamicAnchorinthesameschemaresourcebehaveslikeanormalreftoanchor"
        
def test_Any_array_is_valid(db_conn):
    data = ['foo', 42]
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', '$id': 'https://test.json-schema.org/unmatched-dynamic-anchor/root', '$ref': 'list', '$defs': {'foo': {'$dynamicAnchor': 'items', 'type': 'string'}, 'list': {'$id': 'list', 'type': 'array', 'items': {'$dynamicRef': '#items'}, '$defs': {'items': {'$comment': 'This is only needed to give the reference somewhere to resolve to when it behaves like $ref', '$anchor': 'items', '$dynamicAnchor': 'foo'}}}}}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "AdynamicRefwithanonmatchingdynamicAnchorinthesameschemaresourcebehaveslikeanormalreftoanchor"
        
def test_The_recursive_part_is_valid_against_the_root(db_conn):
    data = {'foo': 'pass', 'bar': {'baz': {'foo': 'pass'}}}
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', '$id': 'https://test.json-schema.org/relative-dynamic-reference/root', '$dynamicAnchor': 'meta', 'type': 'object', 'properties': {'foo': {'const': 'pass'}}, '$ref': 'extended', '$defs': {'extended': {'$id': 'extended', '$dynamicAnchor': 'meta', 'type': 'object', 'properties': {'bar': {'$ref': 'bar'}}}, 'bar': {'$id': 'bar', 'type': 'object', 'properties': {'baz': {'$dynamicRef': 'extended#meta'}}}}}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "AdynamicRefthatinitiallyresolvestoaschemawithamatchingdynamicAnchorresolvestothefirstdynamicAnchorinthedynamicscope"
        
def test_The_recursive_part_is_not_valid_against_the_root(db_conn):
    data = {'foo': 'pass', 'bar': {'baz': {'foo': 'fail'}}}
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', '$id': 'https://test.json-schema.org/relative-dynamic-reference/root', '$dynamicAnchor': 'meta', 'type': 'object', 'properties': {'foo': {'const': 'pass'}}, '$ref': 'extended', '$defs': {'extended': {'$id': 'extended', '$dynamicAnchor': 'meta', 'type': 'object', 'properties': {'bar': {'$ref': 'bar'}}}, 'bar': {'$id': 'bar', 'type': 'object', 'properties': {'baz': {'$dynamicRef': 'extended#meta'}}}}}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "AdynamicRefthatinitiallyresolvestoaschemawithamatchingdynamicAnchorresolvestothefirstdynamicAnchorinthedynamicscope"
        
def test_The_recursive_part_doesnt_need_to_validate_against_the_root(db_conn):
    data = {'foo': 'pass', 'bar': {'baz': {'foo': 'fail'}}}
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', '$id': 'https://test.json-schema.org/relative-dynamic-reference-without-bookend/root', '$dynamicAnchor': 'meta', 'type': 'object', 'properties': {'foo': {'const': 'pass'}}, '$ref': 'extended', '$defs': {'extended': {'$id': 'extended', '$anchor': 'meta', 'type': 'object', 'properties': {'bar': {'$ref': 'bar'}}}, 'bar': {'$id': 'bar', 'type': 'object', 'properties': {'baz': {'$dynamicRef': 'extended#meta'}}}}}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "AdynamicRefthatinitiallyresolvestoaschemawithoutamatchingdynamicAnchorbehaveslikeanormalreftoanchor"
        
def test_number_list_with_number_values(db_conn):
    data = {'kindOfList': 'numbers', 'list': [1.1]}
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', '$id': 'https://test.json-schema.org/dynamic-ref-with-multiple-paths/main', 'if': {'properties': {'kindOfList': {'const': 'numbers'}}, 'required': ['kindOfList']}, 'then': {'$ref': 'numberList'}, 'else': {'$ref': 'stringList'}, '$defs': {'genericList': {'$id': 'genericList', 'properties': {'list': {'items': {'$dynamicRef': '#itemType'}}}, '$defs': {'defaultItemType': {'$comment': 'Only needed to satisfy bookending requirement', '$dynamicAnchor': 'itemType'}}}, 'numberList': {'$id': 'numberList', '$defs': {'itemType': {'$dynamicAnchor': 'itemType', 'type': 'number'}}, '$ref': 'genericList'}, 'stringList': {'$id': 'stringList', '$defs': {'itemType': {'$dynamicAnchor': 'itemType', 'type': 'string'}}, '$ref': 'genericList'}}}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "multipledynamicpathstothedynamicRefkeyword"
        
def test_number_list_with_string_values(db_conn):
    data = {'kindOfList': 'numbers', 'list': ['foo']}
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', '$id': 'https://test.json-schema.org/dynamic-ref-with-multiple-paths/main', 'if': {'properties': {'kindOfList': {'const': 'numbers'}}, 'required': ['kindOfList']}, 'then': {'$ref': 'numberList'}, 'else': {'$ref': 'stringList'}, '$defs': {'genericList': {'$id': 'genericList', 'properties': {'list': {'items': {'$dynamicRef': '#itemType'}}}, '$defs': {'defaultItemType': {'$comment': 'Only needed to satisfy bookending requirement', '$dynamicAnchor': 'itemType'}}}, 'numberList': {'$id': 'numberList', '$defs': {'itemType': {'$dynamicAnchor': 'itemType', 'type': 'number'}}, '$ref': 'genericList'}, 'stringList': {'$id': 'stringList', '$defs': {'itemType': {'$dynamicAnchor': 'itemType', 'type': 'string'}}, '$ref': 'genericList'}}}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "multipledynamicpathstothedynamicRefkeyword"
        
def test_string_list_with_number_values(db_conn):
    data = {'kindOfList': 'strings', 'list': [1.1]}
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', '$id': 'https://test.json-schema.org/dynamic-ref-with-multiple-paths/main', 'if': {'properties': {'kindOfList': {'const': 'numbers'}}, 'required': ['kindOfList']}, 'then': {'$ref': 'numberList'}, 'else': {'$ref': 'stringList'}, '$defs': {'genericList': {'$id': 'genericList', 'properties': {'list': {'items': {'$dynamicRef': '#itemType'}}}, '$defs': {'defaultItemType': {'$comment': 'Only needed to satisfy bookending requirement', '$dynamicAnchor': 'itemType'}}}, 'numberList': {'$id': 'numberList', '$defs': {'itemType': {'$dynamicAnchor': 'itemType', 'type': 'number'}}, '$ref': 'genericList'}, 'stringList': {'$id': 'stringList', '$defs': {'itemType': {'$dynamicAnchor': 'itemType', 'type': 'string'}}, '$ref': 'genericList'}}}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "multipledynamicpathstothedynamicRefkeyword"
        
def test_string_list_with_string_values(db_conn):
    data = {'kindOfList': 'strings', 'list': ['foo']}
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', '$id': 'https://test.json-schema.org/dynamic-ref-with-multiple-paths/main', 'if': {'properties': {'kindOfList': {'const': 'numbers'}}, 'required': ['kindOfList']}, 'then': {'$ref': 'numberList'}, 'else': {'$ref': 'stringList'}, '$defs': {'genericList': {'$id': 'genericList', 'properties': {'list': {'items': {'$dynamicRef': '#itemType'}}}, '$defs': {'defaultItemType': {'$comment': 'Only needed to satisfy bookending requirement', '$dynamicAnchor': 'itemType'}}}, 'numberList': {'$id': 'numberList', '$defs': {'itemType': {'$dynamicAnchor': 'itemType', 'type': 'number'}}, '$ref': 'genericList'}, 'stringList': {'$id': 'stringList', '$defs': {'itemType': {'$dynamicAnchor': 'itemType', 'type': 'string'}}, '$ref': 'genericList'}}}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "multipledynamicpathstothedynamicRefkeyword"
        
def test_string_matches_defsthingy_but_the_dynamicRef_does_not_stop_here(db_conn):
    data = 'a string'
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', '$id': 'https://test.json-schema.org/dynamic-ref-leaving-dynamic-scope/main', 'if': {'$id': 'first_scope', '$defs': {'thingy': {'$comment': 'this is first_scope#thingy', '$dynamicAnchor': 'thingy', 'type': 'number'}}}, 'then': {'$id': 'second_scope', '$ref': 'start', '$defs': {'thingy': {'$comment': 'this is second_scope#thingy, the final destination of the $dynamicRef', '$dynamicAnchor': 'thingy', 'type': 'null'}}}, '$defs': {'start': {'$comment': 'this is the landing spot from $ref', '$id': 'start', '$dynamicRef': 'inner_scope#thingy'}, 'thingy': {'$comment': 'this is the first stop for the $dynamicRef', '$id': 'inner_scope', '$dynamicAnchor': 'thingy', 'type': 'string'}}}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "afterleavingadynamicscopeitisnotusedbyadynamicRef"
        
def test_first_scope_is_not_in_dynamic_scope_for_the_dynamicRef(db_conn):
    data = 42
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', '$id': 'https://test.json-schema.org/dynamic-ref-leaving-dynamic-scope/main', 'if': {'$id': 'first_scope', '$defs': {'thingy': {'$comment': 'this is first_scope#thingy', '$dynamicAnchor': 'thingy', 'type': 'number'}}}, 'then': {'$id': 'second_scope', '$ref': 'start', '$defs': {'thingy': {'$comment': 'this is second_scope#thingy, the final destination of the $dynamicRef', '$dynamicAnchor': 'thingy', 'type': 'null'}}}, '$defs': {'start': {'$comment': 'this is the landing spot from $ref', '$id': 'start', '$dynamicRef': 'inner_scope#thingy'}, 'thingy': {'$comment': 'this is the first stop for the $dynamicRef', '$id': 'inner_scope', '$dynamicAnchor': 'thingy', 'type': 'string'}}}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "afterleavingadynamicscopeitisnotusedbyadynamicRef"
        
def test_thendefsthingy_is_the_final_stop_for_the_dynamicRef(db_conn):
    data = null
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', '$id': 'https://test.json-schema.org/dynamic-ref-leaving-dynamic-scope/main', 'if': {'$id': 'first_scope', '$defs': {'thingy': {'$comment': 'this is first_scope#thingy', '$dynamicAnchor': 'thingy', 'type': 'number'}}}, 'then': {'$id': 'second_scope', '$ref': 'start', '$defs': {'thingy': {'$comment': 'this is second_scope#thingy, the final destination of the $dynamicRef', '$dynamicAnchor': 'thingy', 'type': 'null'}}}, '$defs': {'start': {'$comment': 'this is the landing spot from $ref', '$id': 'start', '$dynamicRef': 'inner_scope#thingy'}, 'thingy': {'$comment': 'this is the first stop for the $dynamicRef', '$id': 'inner_scope', '$dynamicAnchor': 'thingy', 'type': 'string'}}}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "afterleavingadynamicscopeitisnotusedbyadynamicRef"
        
def test_instance_with_misspelled_field(db_conn):
    data = {'children': [{'daat': 1}]}
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', '$id': 'http://localhost:1234/draft2020-12/strict-tree.json', '$dynamicAnchor': 'node', '$ref': 'tree.json', 'unevaluatedProperties': False}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "stricttreeschemaguardsagainstmisspelledproperties"
        
def test_instance_with_correct_field(db_conn):
    data = {'children': [{'data': 1}]}
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', '$id': 'http://localhost:1234/draft2020-12/strict-tree.json', '$dynamicAnchor': 'node', '$ref': 'tree.json', 'unevaluatedProperties': False}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "stricttreeschemaguardsagainstmisspelledproperties"
        
def test_incorrect_parent_schema(db_conn):
    data = {'a': True}
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', '$id': 'http://localhost:1234/draft2020-12/strict-extendible.json', '$ref': 'extendible-dynamic-ref.json', '$defs': {'elements': {'$dynamicAnchor': 'elements', 'properties': {'a': True}, 'required': ['a'], 'additionalProperties': False}}}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "testsforimplementationdynamicanchorandreferencelink"
        
def test_incorrect_extended_schema(db_conn):
    data = {'elements': [{'b': 1}]}
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', '$id': 'http://localhost:1234/draft2020-12/strict-extendible.json', '$ref': 'extendible-dynamic-ref.json', '$defs': {'elements': {'$dynamicAnchor': 'elements', 'properties': {'a': True}, 'required': ['a'], 'additionalProperties': False}}}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "testsforimplementationdynamicanchorandreferencelink"
        
def test_correct_extended_schema(db_conn):
    data = {'elements': [{'a': 1}]}
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', '$id': 'http://localhost:1234/draft2020-12/strict-extendible.json', '$ref': 'extendible-dynamic-ref.json', '$defs': {'elements': {'$dynamicAnchor': 'elements', 'properties': {'a': True}, 'required': ['a'], 'additionalProperties': False}}}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "testsforimplementationdynamicanchorandreferencelink"
        
def test_incorrect_parent_schema(db_conn):
    data = {'a': True}
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', '$id': 'http://localhost:1234/draft2020-12/strict-extendible-allof-defs-first.json', 'allOf': [{'$ref': 'extendible-dynamic-ref.json'}, {'$defs': {'elements': {'$dynamicAnchor': 'elements', 'properties': {'a': True}, 'required': ['a'], 'additionalProperties': False}}}]}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "refanddynamicAnchorareindependentoforderdefsfirst"
        
def test_incorrect_extended_schema(db_conn):
    data = {'elements': [{'b': 1}]}
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', '$id': 'http://localhost:1234/draft2020-12/strict-extendible-allof-defs-first.json', 'allOf': [{'$ref': 'extendible-dynamic-ref.json'}, {'$defs': {'elements': {'$dynamicAnchor': 'elements', 'properties': {'a': True}, 'required': ['a'], 'additionalProperties': False}}}]}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "refanddynamicAnchorareindependentoforderdefsfirst"
        
def test_correct_extended_schema(db_conn):
    data = {'elements': [{'a': 1}]}
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', '$id': 'http://localhost:1234/draft2020-12/strict-extendible-allof-defs-first.json', 'allOf': [{'$ref': 'extendible-dynamic-ref.json'}, {'$defs': {'elements': {'$dynamicAnchor': 'elements', 'properties': {'a': True}, 'required': ['a'], 'additionalProperties': False}}}]}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "refanddynamicAnchorareindependentoforderdefsfirst"
        
def test_incorrect_parent_schema(db_conn):
    data = {'a': True}
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', '$id': 'http://localhost:1234/draft2020-12/strict-extendible-allof-ref-first.json', 'allOf': [{'$defs': {'elements': {'$dynamicAnchor': 'elements', 'properties': {'a': True}, 'required': ['a'], 'additionalProperties': False}}}, {'$ref': 'extendible-dynamic-ref.json'}]}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "refanddynamicAnchorareindependentoforderreffirst"
        
def test_incorrect_extended_schema(db_conn):
    data = {'elements': [{'b': 1}]}
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', '$id': 'http://localhost:1234/draft2020-12/strict-extendible-allof-ref-first.json', 'allOf': [{'$defs': {'elements': {'$dynamicAnchor': 'elements', 'properties': {'a': True}, 'required': ['a'], 'additionalProperties': False}}}, {'$ref': 'extendible-dynamic-ref.json'}]}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "refanddynamicAnchorareindependentoforderreffirst"
        
def test_correct_extended_schema(db_conn):
    data = {'elements': [{'a': 1}]}
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', '$id': 'http://localhost:1234/draft2020-12/strict-extendible-allof-ref-first.json', 'allOf': [{'$defs': {'elements': {'$dynamicAnchor': 'elements', 'properties': {'a': True}, 'required': ['a'], 'additionalProperties': False}}}, {'$ref': 'extendible-dynamic-ref.json'}]}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "refanddynamicAnchorareindependentoforderreffirst"
        
def test_number_is_valid(db_conn):
    data = 1
    schema = {'$ref': 'http://localhost:1234/draft2020-12/detached-dynamicref.json#/$defs/foo'}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "reftodynamicReffindsdetacheddynamicAnchor"
        
def test_nonnumber_is_invalid(db_conn):
    data = 'a'
    schema = {'$ref': 'http://localhost:1234/draft2020-12/detached-dynamicref.json#/$defs/foo'}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "reftodynamicReffindsdetacheddynamicAnchor"
        
def test_follow_dynamicRef_to_a_true_schema(db_conn):
    data = {'true': 1}
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', '$defs': {'true': True, 'false': False}, 'properties': {'true': {'$dynamicRef': '#/$defs/true'}, 'false': {'$dynamicRef': '#/$defs/false'}}}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "dynamicRefpointstoabooleanschema"
        
def test_follow_dynamicRef_to_a_false_schema(db_conn):
    data = {'false': 1}
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', '$defs': {'true': True, 'false': False}, 'properties': {'true': {'$dynamicRef': '#/$defs/true'}, 'false': {'$dynamicRef': '#/$defs/false'}}}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "dynamicRefpointstoabooleanschema"
        