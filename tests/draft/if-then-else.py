
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


def test_valid_when_valid_against_lone_if(db_conn):
    data = 0
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'if': {'const': 0}}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "ignoreifwithoutthenorelse"
        
def test_valid_when_invalid_against_lone_if(db_conn):
    data = 'hello'
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'if': {'const': 0}}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "ignoreifwithoutthenorelse"
        
def test_valid_when_valid_against_lone_then(db_conn):
    data = 0
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'then': {'const': 0}}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "ignorethenwithoutif"
        
def test_valid_when_invalid_against_lone_then(db_conn):
    data = 'hello'
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'then': {'const': 0}}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "ignorethenwithoutif"
        
def test_valid_when_valid_against_lone_else(db_conn):
    data = 0
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'else': {'const': 0}}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "ignoreelsewithoutif"
        
def test_valid_when_invalid_against_lone_else(db_conn):
    data = 'hello'
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'else': {'const': 0}}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "ignoreelsewithoutif"
        
def test_valid_through_then(db_conn):
    data = -1
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'if': {'exclusiveMaximum': 0}, 'then': {'minimum': -10}}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "ifandthenwithoutelse"
        
def test_invalid_through_then(db_conn):
    data = -100
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'if': {'exclusiveMaximum': 0}, 'then': {'minimum': -10}}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "ifandthenwithoutelse"
        
def test_valid_when_if_test_fails(db_conn):
    data = 3
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'if': {'exclusiveMaximum': 0}, 'then': {'minimum': -10}}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "ifandthenwithoutelse"
        
def test_valid_when_if_test_passes(db_conn):
    data = -1
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'if': {'exclusiveMaximum': 0}, 'else': {'multipleOf': 2}}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "ifandelsewithoutthen"
        
def test_valid_through_else(db_conn):
    data = 4
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'if': {'exclusiveMaximum': 0}, 'else': {'multipleOf': 2}}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "ifandelsewithoutthen"
        
def test_invalid_through_else(db_conn):
    data = 3
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'if': {'exclusiveMaximum': 0}, 'else': {'multipleOf': 2}}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "ifandelsewithoutthen"
        
def test_valid_through_then(db_conn):
    data = -1
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'if': {'exclusiveMaximum': 0}, 'then': {'minimum': -10}, 'else': {'multipleOf': 2}}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "validateagainstcorrectbranchthenvselse"
        
def test_invalid_through_then(db_conn):
    data = -100
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'if': {'exclusiveMaximum': 0}, 'then': {'minimum': -10}, 'else': {'multipleOf': 2}}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "validateagainstcorrectbranchthenvselse"
        
def test_valid_through_else(db_conn):
    data = 4
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'if': {'exclusiveMaximum': 0}, 'then': {'minimum': -10}, 'else': {'multipleOf': 2}}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "validateagainstcorrectbranchthenvselse"
        
def test_invalid_through_else(db_conn):
    data = 3
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'if': {'exclusiveMaximum': 0}, 'then': {'minimum': -10}, 'else': {'multipleOf': 2}}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "validateagainstcorrectbranchthenvselse"
        
def test_valid_but_would_have_been_invalid_through_then(db_conn):
    data = -100
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'allOf': [{'if': {'exclusiveMaximum': 0}}, {'then': {'minimum': -10}}, {'else': {'multipleOf': 2}}]}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "noninterferenceacrosscombinedschemas"
        
def test_valid_but_would_have_been_invalid_through_else(db_conn):
    data = 3
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'allOf': [{'if': {'exclusiveMaximum': 0}}, {'then': {'minimum': -10}}, {'else': {'multipleOf': 2}}]}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "noninterferenceacrosscombinedschemas"
        
def test_boolean_schema_true_in_if_always_chooses_the_then_path_valid(db_conn):
    data = 'then'
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'if': True, 'then': {'const': 'then'}, 'else': {'const': 'else'}}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "ifwithbooleanschematrue"
        
def test_boolean_schema_true_in_if_always_chooses_the_then_path_invalid(db_conn):
    data = 'else'
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'if': True, 'then': {'const': 'then'}, 'else': {'const': 'else'}}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "ifwithbooleanschematrue"
        
def test_boolean_schema_false_in_if_always_chooses_the_else_path_invalid(db_conn):
    data = 'then'
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'if': False, 'then': {'const': 'then'}, 'else': {'const': 'else'}}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "ifwithbooleanschemafalse"
        
def test_boolean_schema_false_in_if_always_chooses_the_else_path_valid(db_conn):
    data = 'else'
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'if': False, 'then': {'const': 'then'}, 'else': {'const': 'else'}}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "ifwithbooleanschemafalse"
        
def test_yes_redirects_to_then_and_passes(db_conn):
    data = 'yes'
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'then': {'const': 'yes'}, 'else': {'const': 'other'}, 'if': {'maxLength': 4}}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "ifappearsattheendwhenserializedkeywordprocessingsequence"
        
def test_other_redirects_to_else_and_passes(db_conn):
    data = 'other'
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'then': {'const': 'yes'}, 'else': {'const': 'other'}, 'if': {'maxLength': 4}}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "ifappearsattheendwhenserializedkeywordprocessingsequence"
        
def test_no_redirects_to_then_and_fails(db_conn):
    data = 'no'
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'then': {'const': 'yes'}, 'else': {'const': 'other'}, 'if': {'maxLength': 4}}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "ifappearsattheendwhenserializedkeywordprocessingsequence"
        
def test_invalid_redirects_to_else_and_fails(db_conn):
    data = 'invalid'
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'then': {'const': 'yes'}, 'else': {'const': 'other'}, 'if': {'maxLength': 4}}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "ifappearsattheendwhenserializedkeywordprocessingsequence"
        