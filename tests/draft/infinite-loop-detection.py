
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


def test_passing_case(db_conn):
    data = {'foo': 1}
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', '$defs': {'int': {'type': 'integer'}}, 'allOf': [{'properties': {'foo': {'$ref': '#/$defs/int'}}}, {'additionalProperties': {'$ref': '#/$defs/int'}}]}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "evaluatingthesameschemalocationagainstthesamedatalocationtwiceisnotasignofaninfiniteloop"
        
def test_failing_case(db_conn):
    data = {'foo': 'a string'}
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', '$defs': {'int': {'type': 'integer'}}, 'allOf': [{'properties': {'foo': {'$ref': '#/$defs/int'}}}, {'additionalProperties': {'$ref': '#/$defs/int'}}]}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "evaluatingthesameschemalocationagainstthesamedatalocationtwiceisnotasignofaninfiniteloop"
        