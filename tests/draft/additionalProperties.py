
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


def test_no_additional_properties_is_valid(db_conn):
    data = {'foo': 1}
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'properties': {'foo': {}, 'bar': {}}, 'patternProperties': {'^v': {}}, 'additionalProperties': False}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "additionalPropertiesbeingfalsedoesnotallowotherproperties"
        
def test_an_additional_property_is_invalid(db_conn):
    data = {'foo': 1, 'bar': 2, 'quux': 'boom'}
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'properties': {'foo': {}, 'bar': {}}, 'patternProperties': {'^v': {}}, 'additionalProperties': False}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "additionalPropertiesbeingfalsedoesnotallowotherproperties"
        
def test_ignores_arrays(db_conn):
    data = [1, 2, 3]
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'properties': {'foo': {}, 'bar': {}}, 'patternProperties': {'^v': {}}, 'additionalProperties': False}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "additionalPropertiesbeingfalsedoesnotallowotherproperties"
        
def test_ignores_strings(db_conn):
    data = 'foobarbaz'
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'properties': {'foo': {}, 'bar': {}}, 'patternProperties': {'^v': {}}, 'additionalProperties': False}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "additionalPropertiesbeingfalsedoesnotallowotherproperties"
        
def test_ignores_other_nonobjects(db_conn):
    data = 12
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'properties': {'foo': {}, 'bar': {}}, 'patternProperties': {'^v': {}}, 'additionalProperties': False}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "additionalPropertiesbeingfalsedoesnotallowotherproperties"
        
def test_patternProperties_are_not_additional_properties(db_conn):
    data = {'foo': 1, 'vroom': 2}
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'properties': {'foo': {}, 'bar': {}}, 'patternProperties': {'^v': {}}, 'additionalProperties': False}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "additionalPropertiesbeingfalsedoesnotallowotherproperties"
        
def test_matching_the_pattern_is_valid(db_conn):
    data = {'ármányos': 2}
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'patternProperties': {'^á': {}}, 'additionalProperties': False}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "nonASCIIpatternwithadditionalProperties"
        
def test_not_matching_the_pattern_is_invalid(db_conn):
    data = {'élmény': 2}
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'patternProperties': {'^á': {}}, 'additionalProperties': False}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "nonASCIIpatternwithadditionalProperties"
        
def test_no_additional_properties_is_valid(db_conn):
    data = {'foo': 1}
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'properties': {'foo': {}, 'bar': {}}, 'additionalProperties': {'type': 'boolean'}}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "additionalPropertieswithschema"
        
def test_an_additional_valid_property_is_valid(db_conn):
    data = {'foo': 1, 'bar': 2, 'quux': True}
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'properties': {'foo': {}, 'bar': {}}, 'additionalProperties': {'type': 'boolean'}}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "additionalPropertieswithschema"
        
def test_an_additional_invalid_property_is_invalid(db_conn):
    data = {'foo': 1, 'bar': 2, 'quux': 12}
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'properties': {'foo': {}, 'bar': {}}, 'additionalProperties': {'type': 'boolean'}}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "additionalPropertieswithschema"
        
def test_an_additional_valid_property_is_valid(db_conn):
    data = {'foo': True}
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'additionalProperties': {'type': 'boolean'}}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "additionalPropertiescanexistbyitself"
        
def test_an_additional_invalid_property_is_invalid(db_conn):
    data = {'foo': 1}
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'additionalProperties': {'type': 'boolean'}}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "additionalPropertiescanexistbyitself"
        
def test_additional_properties_are_allowed(db_conn):
    data = {'foo': 1, 'bar': 2, 'quux': True}
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'properties': {'foo': {}, 'bar': {}}}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "additionalPropertiesareallowedbydefault"
        
def test_properties_defined_in_allOf_are_not_examined(db_conn):
    data = {'foo': 1, 'bar': True}
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'allOf': [{'properties': {'foo': {}}}], 'additionalProperties': {'type': 'boolean'}}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "additionalPropertiesdoesnotlookinapplicators"
        
def test_allows_null_values(db_conn):
    data = {'foo': None}
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'additionalProperties': {'type': 'null'}}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "additionalPropertieswithnullvaluedinstanceproperties"
        