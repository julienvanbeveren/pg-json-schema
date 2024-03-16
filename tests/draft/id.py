
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


def test_Identifier_name(db_conn):
    data = {'$ref': '#foo', '$defs': {'A': {'$id': '#foo', 'type': 'integer'}}}
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', '$ref': 'https://json-schema.org/draft/2020-12/schema'}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "Invaliduseoffragmentsinlocationindependentid"
        
def test_Identifier_name_and_no_ref(db_conn):
    data = {'$defs': {'A': {'$id': '#foo'}}}
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', '$ref': 'https://json-schema.org/draft/2020-12/schema'}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "Invaliduseoffragmentsinlocationindependentid"
        
def test_Identifier_path(db_conn):
    data = {'$ref': '#/a/b', '$defs': {'A': {'$id': '#/a/b', 'type': 'integer'}}}
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', '$ref': 'https://json-schema.org/draft/2020-12/schema'}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "Invaliduseoffragmentsinlocationindependentid"
        
def test_Identifier_name_with_absolute_URI(db_conn):
    data = {'$ref': 'http://localhost:1234/draft2020-12/bar#foo', '$defs': {'A': {'$id': 'http://localhost:1234/draft2020-12/bar#foo', 'type': 'integer'}}}
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', '$ref': 'https://json-schema.org/draft/2020-12/schema'}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "Invaliduseoffragmentsinlocationindependentid"
        
def test_Identifier_path_with_absolute_URI(db_conn):
    data = {'$ref': 'http://localhost:1234/draft2020-12/bar#/a/b', '$defs': {'A': {'$id': 'http://localhost:1234/draft2020-12/bar#/a/b', 'type': 'integer'}}}
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', '$ref': 'https://json-schema.org/draft/2020-12/schema'}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "Invaliduseoffragmentsinlocationindependentid"
        
def test_Identifier_name_with_base_URI_change_in_subschema(db_conn):
    data = {'$id': 'http://localhost:1234/draft2020-12/root', '$ref': 'http://localhost:1234/draft2020-12/nested.json#foo', '$defs': {'A': {'$id': 'nested.json', '$defs': {'B': {'$id': '#foo', 'type': 'integer'}}}}}
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', '$ref': 'https://json-schema.org/draft/2020-12/schema'}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "Invaliduseoffragmentsinlocationindependentid"
        
def test_Identifier_path_with_base_URI_change_in_subschema(db_conn):
    data = {'$id': 'http://localhost:1234/draft2020-12/root', '$ref': 'http://localhost:1234/draft2020-12/nested.json#/a/b', '$defs': {'A': {'$id': 'nested.json', '$defs': {'B': {'$id': '#/a/b', 'type': 'integer'}}}}}
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', '$ref': 'https://json-schema.org/draft/2020-12/schema'}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "Invaliduseoffragmentsinlocationindependentid"
        
def test_Identifier_name_with_absolute_URI(db_conn):
    data = {'$ref': 'http://localhost:1234/draft2020-12/bar', '$defs': {'A': {'$id': 'http://localhost:1234/draft2020-12/bar#', 'type': 'integer'}}}
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', '$ref': 'https://json-schema.org/draft/2020-12/schema'}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "Validuseofemptyfragmentsinlocationindependentid"
        
def test_Identifier_name_with_base_URI_change_in_subschema(db_conn):
    data = {'$id': 'http://localhost:1234/draft2020-12/root', '$ref': 'http://localhost:1234/draft2020-12/nested.json#/$defs/B', '$defs': {'A': {'$id': 'nested.json', '$defs': {'B': {'$id': '#', 'type': 'integer'}}}}}
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', '$ref': 'https://json-schema.org/draft/2020-12/schema'}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "Validuseofemptyfragmentsinlocationindependentid"
        
def test_Unnormalized_identifier(db_conn):
    data = {'$ref': 'http://localhost:1234/draft2020-12/foo/baz', '$defs': {'A': {'$id': 'http://localhost:1234/draft2020-12/foo/bar/../baz', 'type': 'integer'}}}
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', '$ref': 'https://json-schema.org/draft/2020-12/schema'}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "Unnormalizedidsareallowedbutdiscouraged"
        
def test_Unnormalized_identifier_and_no_ref(db_conn):
    data = {'$defs': {'A': {'$id': 'http://localhost:1234/draft2020-12/foo/bar/../baz', 'type': 'integer'}}}
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', '$ref': 'https://json-schema.org/draft/2020-12/schema'}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "Unnormalizedidsareallowedbutdiscouraged"
        
def test_Unnormalized_identifier_with_empty_fragment(db_conn):
    data = {'$ref': 'http://localhost:1234/draft2020-12/foo/baz', '$defs': {'A': {'$id': 'http://localhost:1234/draft2020-12/foo/bar/../baz#', 'type': 'integer'}}}
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', '$ref': 'https://json-schema.org/draft/2020-12/schema'}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "Unnormalizedidsareallowedbutdiscouraged"
        
def test_Unnormalized_identifier_with_empty_fragment_and_no_ref(db_conn):
    data = {'$defs': {'A': {'$id': 'http://localhost:1234/draft2020-12/foo/bar/../baz#', 'type': 'integer'}}}
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', '$ref': 'https://json-schema.org/draft/2020-12/schema'}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "Unnormalizedidsareallowedbutdiscouraged"
        