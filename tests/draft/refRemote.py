
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


def test_remote_ref_valid(db_conn):
    data = 1
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', '$ref': 'http://localhost:1234/draft2020-12/integer.json'}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "remoteref"
        
def test_remote_ref_invalid(db_conn):
    data = 'a'
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', '$ref': 'http://localhost:1234/draft2020-12/integer.json'}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "remoteref"
        
def test_remote_fragment_valid(db_conn):
    data = 1
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', '$ref': 'http://localhost:1234/draft2020-12/subSchemas.json#/$defs/integer'}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "fragmentwithinremoteref"
        
def test_remote_fragment_invalid(db_conn):
    data = 'a'
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', '$ref': 'http://localhost:1234/draft2020-12/subSchemas.json#/$defs/integer'}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "fragmentwithinremoteref"
        
def test_remote_anchor_valid(db_conn):
    data = 1
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', '$ref': 'http://localhost:1234/draft2020-12/locationIndependentIdentifier.json#foo'}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "anchorwithinremoteref"
        
def test_remote_anchor_invalid(db_conn):
    data = 'a'
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', '$ref': 'http://localhost:1234/draft2020-12/locationIndependentIdentifier.json#foo'}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "anchorwithinremoteref"
        
def test_ref_within_ref_valid(db_conn):
    data = 1
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', '$ref': 'http://localhost:1234/draft2020-12/subSchemas.json#/$defs/refToInteger'}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "refwithinremoteref"
        
def test_ref_within_ref_invalid(db_conn):
    data = 'a'
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', '$ref': 'http://localhost:1234/draft2020-12/subSchemas.json#/$defs/refToInteger'}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "refwithinremoteref"
        
def test_base_URI_change_ref_valid(db_conn):
    data = [[1]]
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', '$id': 'http://localhost:1234/draft2020-12/', 'items': {'$id': 'baseUriChange/', 'items': {'$ref': 'folderInteger.json'}}}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "baseURIchange"
        
def test_base_URI_change_ref_invalid(db_conn):
    data = [['a']]
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', '$id': 'http://localhost:1234/draft2020-12/', 'items': {'$id': 'baseUriChange/', 'items': {'$ref': 'folderInteger.json'}}}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "baseURIchange"
        
def test_number_is_valid(db_conn):
    data = {'list': [1]}
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', '$id': 'http://localhost:1234/draft2020-12/scope_change_defs1.json', 'type': 'object', 'properties': {'list': {'$ref': 'baseUriChangeFolder/'}}, '$defs': {'baz': {'$id': 'baseUriChangeFolder/', 'type': 'array', 'items': {'$ref': 'folderInteger.json'}}}}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "baseURIchangechangefolder"
        
def test_string_is_invalid(db_conn):
    data = {'list': ['a']}
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', '$id': 'http://localhost:1234/draft2020-12/scope_change_defs1.json', 'type': 'object', 'properties': {'list': {'$ref': 'baseUriChangeFolder/'}}, '$defs': {'baz': {'$id': 'baseUriChangeFolder/', 'type': 'array', 'items': {'$ref': 'folderInteger.json'}}}}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "baseURIchangechangefolder"
        
def test_number_is_valid(db_conn):
    data = {'list': [1]}
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', '$id': 'http://localhost:1234/draft2020-12/scope_change_defs2.json', 'type': 'object', 'properties': {'list': {'$ref': 'baseUriChangeFolderInSubschema/#/$defs/bar'}}, '$defs': {'baz': {'$id': 'baseUriChangeFolderInSubschema/', '$defs': {'bar': {'type': 'array', 'items': {'$ref': 'folderInteger.json'}}}}}}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "baseURIchangechangefolderinsubschema"
        
def test_string_is_invalid(db_conn):
    data = {'list': ['a']}
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', '$id': 'http://localhost:1234/draft2020-12/scope_change_defs2.json', 'type': 'object', 'properties': {'list': {'$ref': 'baseUriChangeFolderInSubschema/#/$defs/bar'}}, '$defs': {'baz': {'$id': 'baseUriChangeFolderInSubschema/', '$defs': {'bar': {'type': 'array', 'items': {'$ref': 'folderInteger.json'}}}}}}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "baseURIchangechangefolderinsubschema"
        
def test_string_is_valid(db_conn):
    data = {'name': 'foo'}
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', '$id': 'http://localhost:1234/draft2020-12/object', 'type': 'object', 'properties': {'name': {'$ref': 'name-defs.json#/$defs/orNull'}}}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "rootrefinremoteref"
        
def test_null_is_valid(db_conn):
    data = {'name': None}
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', '$id': 'http://localhost:1234/draft2020-12/object', 'type': 'object', 'properties': {'name': {'$ref': 'name-defs.json#/$defs/orNull'}}}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "rootrefinremoteref"
        
def test_object_is_invalid(db_conn):
    data = {'name': {'name': None}}
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', '$id': 'http://localhost:1234/draft2020-12/object', 'type': 'object', 'properties': {'name': {'$ref': 'name-defs.json#/$defs/orNull'}}}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "rootrefinremoteref"
        
def test_invalid(db_conn):
    data = {'bar': 1}
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', '$id': 'http://localhost:1234/draft2020-12/schema-remote-ref-ref-defs1.json', '$ref': 'ref-and-defs.json'}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "remoterefwithreftodefs"
        
def test_valid(db_conn):
    data = {'bar': 'a'}
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', '$id': 'http://localhost:1234/draft2020-12/schema-remote-ref-ref-defs1.json', '$ref': 'ref-and-defs.json'}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "remoterefwithreftodefs"
        
def test_integer_is_valid(db_conn):
    data = 1
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', '$ref': 'http://localhost:1234/draft2020-12/locationIndependentIdentifier.json#/$defs/refToInteger'}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "Locationindependentidentifierinremoteref"
        
def test_string_is_invalid(db_conn):
    data = 'foo'
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', '$ref': 'http://localhost:1234/draft2020-12/locationIndependentIdentifier.json#/$defs/refToInteger'}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "Locationindependentidentifierinremoteref"
        
def test_number_is_invalid(db_conn):
    data = {'name': {'foo': 1}}
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', '$id': 'http://localhost:1234/draft2020-12/some-id', 'properties': {'name': {'$ref': 'nested/foo-ref-string.json'}}}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "retrievednestedrefsresolverelativetotheirURInotid"
        
def test_string_is_valid(db_conn):
    data = {'name': {'foo': 'a'}}
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', '$id': 'http://localhost:1234/draft2020-12/some-id', 'properties': {'name': {'$ref': 'nested/foo-ref-string.json'}}}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "retrievednestedrefsresolverelativetotheirURInotid"
        
def test_number_is_invalid(db_conn):
    data = 1
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', '$ref': 'http://localhost:1234/different-id-ref-string.json'}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "remoteHTTPrefwithdifferentid"
        
def test_string_is_valid(db_conn):
    data = 'foo'
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', '$ref': 'http://localhost:1234/different-id-ref-string.json'}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "remoteHTTPrefwithdifferentid"
        
def test_number_is_invalid(db_conn):
    data = 1
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', '$ref': 'http://localhost:1234/urn-ref-string.json'}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "remoteHTTPrefwithdifferentURNid"
        
def test_string_is_valid(db_conn):
    data = 'foo'
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', '$ref': 'http://localhost:1234/urn-ref-string.json'}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "remoteHTTPrefwithdifferentURNid"
        
def test_number_is_invalid(db_conn):
    data = 1
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', '$ref': 'http://localhost:1234/nested-absolute-ref-to-string.json'}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "remoteHTTPrefwithnestedabsoluteref"
        
def test_string_is_valid(db_conn):
    data = 'foo'
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', '$ref': 'http://localhost:1234/nested-absolute-ref-to-string.json'}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "remoteHTTPrefwithnestedabsoluteref"
        
def test_number_is_valid(db_conn):
    data = 1
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', '$ref': 'http://localhost:1234/draft2020-12/detached-ref.json#/$defs/foo'}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "reftoreffindsdetachedanchor"
        
def test_nonnumber_is_invalid(db_conn):
    data = 'a'
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', '$ref': 'http://localhost:1234/draft2020-12/detached-ref.json#/$defs/foo'}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "reftoreffindsdetachedanchor"
        