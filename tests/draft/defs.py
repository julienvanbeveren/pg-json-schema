
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


def test_valid_definition_schema(db_conn):
    data = {"$defs": {"foo": {"type": "integer"}}}
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

    assert result is True, "validatedefinitionagainstmetaschema"
        
def test_invalid_definition_schema(db_conn):
    data = {"$defs": {"foo": {"type": 1}}}
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

    assert result is False, "validatedefinitionagainstmetaschema"
        