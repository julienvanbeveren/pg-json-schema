
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


def test_match(db_conn):
    data = 1
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "$ref": "#foo",
    "$defs": {
        "A": {
            "$anchor": "foo",
            "type": "integer"
        }
    }
}

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "Locationindependentidentifier"
        
def test_mismatch(db_conn):
    data = 'a'
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "$ref": "#foo",
    "$defs": {
        "A": {
            "$anchor": "foo",
            "type": "integer"
        }
    }
}

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "Locationindependentidentifier"
        
def test_match(db_conn):
    data = 1
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "$ref": "http://localhost:1234/draft2020-12/bar#foo",
    "$defs": {
        "A": {
            "$id": "http://localhost:1234/draft2020-12/bar",
            "$anchor": "foo",
            "type": "integer"
        }
    }
}

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "LocationindependentidentifierwithabsoluteURI"
        
def test_mismatch(db_conn):
    data = 'a'
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "$ref": "http://localhost:1234/draft2020-12/bar#foo",
    "$defs": {
        "A": {
            "$id": "http://localhost:1234/draft2020-12/bar",
            "$anchor": "foo",
            "type": "integer"
        }
    }
}

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "LocationindependentidentifierwithabsoluteURI"
        
def test_match(db_conn):
    data = 1
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "$id": "http://localhost:1234/draft2020-12/root",
    "$ref": "http://localhost:1234/draft2020-12/nested.json#foo",
    "$defs": {
        "A": {
            "$id": "nested.json",
            "$defs": {
                "B": {
                    "$anchor": "foo",
                    "type": "integer"
                }
            }
        }
    }
}

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "LocationindependentidentifierwithbaseURIchangeinsubschema"
        
def test_mismatch(db_conn):
    data = 'a'
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "$id": "http://localhost:1234/draft2020-12/root",
    "$ref": "http://localhost:1234/draft2020-12/nested.json#foo",
    "$defs": {
        "A": {
            "$id": "nested.json",
            "$defs": {
                "B": {
                    "$anchor": "foo",
                    "type": "integer"
                }
            }
        }
    }
}

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "LocationindependentidentifierwithbaseURIchangeinsubschema"
        
def test_ref_resolves_to_defsAallOf1(db_conn):
    data = 'a'
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "$id": "http://localhost:1234/draft2020-12/foobar",
    "$defs": {
        "A": {
            "$id": "child1",
            "allOf": [
                {
                    "$id": "child2",
                    "$anchor": "my_anchor",
                    "type": "number"
                },
                {
                    "$anchor": "my_anchor",
                    "type": "string"
                }
            ]
        }
    },
    "$ref": "child1#my_anchor"
}

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "sameanchorwithdifferentbaseuri"
        
def test_ref_does_not_resolve_to_defsAallOf0(db_conn):
    data = 1
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "$id": "http://localhost:1234/draft2020-12/foobar",
    "$defs": {
        "A": {
            "$id": "child1",
            "allOf": [
                {
                    "$id": "child2",
                    "$anchor": "my_anchor",
                    "type": "number"
                },
                {
                    "$anchor": "my_anchor",
                    "type": "string"
                }
            ]
        }
    },
    "$ref": "child1#my_anchor"
}

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "sameanchorwithdifferentbaseuri"
        
def test_MUST_start_with_a_letter_and_not_(db_conn):
    data = {"$anchor": "#foo"}
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "$ref": "https://json-schema.org/draft/2020-12/schema"
}

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "invalidanchors"
        
def test_JSON_pointers_are_not_valid(db_conn):
    data = {"$anchor": "/a/b"}
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "$ref": "https://json-schema.org/draft/2020-12/schema"
}

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "invalidanchors"
        
def test_invalid_with_valid_beginning(db_conn):
    data = {"$anchor": "foo#something"}
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "$ref": "https://json-schema.org/draft/2020-12/schema"
}

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "invalidanchors"
        