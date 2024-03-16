
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


def test_a_single_valid_match_is_valid(db_conn):
    data = {'foo': 1}
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'patternProperties': {'f.*o': {'type': 'integer'}}}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "patternPropertiesvalidatespropertiesmatchingaregex"
        
def test_multiple_valid_matches_is_valid(db_conn):
    data = {'foo': 1, 'foooooo': 2}
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'patternProperties': {'f.*o': {'type': 'integer'}}}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "patternPropertiesvalidatespropertiesmatchingaregex"
        
def test_a_single_invalid_match_is_invalid(db_conn):
    data = {'foo': 'bar', 'fooooo': 2}
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'patternProperties': {'f.*o': {'type': 'integer'}}}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "patternPropertiesvalidatespropertiesmatchingaregex"
        
def test_multiple_invalid_matches_is_invalid(db_conn):
    data = {'foo': 'bar', 'foooooo': 'baz'}
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'patternProperties': {'f.*o': {'type': 'integer'}}}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "patternPropertiesvalidatespropertiesmatchingaregex"
        
def test_ignores_arrays(db_conn):
    data = ['foo']
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'patternProperties': {'f.*o': {'type': 'integer'}}}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "patternPropertiesvalidatespropertiesmatchingaregex"
        
def test_ignores_strings(db_conn):
    data = 'foo'
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'patternProperties': {'f.*o': {'type': 'integer'}}}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "patternPropertiesvalidatespropertiesmatchingaregex"
        
def test_ignores_other_nonobjects(db_conn):
    data = 12
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'patternProperties': {'f.*o': {'type': 'integer'}}}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "patternPropertiesvalidatespropertiesmatchingaregex"
        
def test_a_single_valid_match_is_valid(db_conn):
    data = {'a': 21}
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'patternProperties': {'a*': {'type': 'integer'}, 'aaa*': {'maximum': 20}}}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "multiplesimultaneouspatternPropertiesarevalidated"
        
def test_a_simultaneous_match_is_valid(db_conn):
    data = {'aaaa': 18}
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'patternProperties': {'a*': {'type': 'integer'}, 'aaa*': {'maximum': 20}}}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "multiplesimultaneouspatternPropertiesarevalidated"
        
def test_multiple_matches_is_valid(db_conn):
    data = {'a': 21, 'aaaa': 18}
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'patternProperties': {'a*': {'type': 'integer'}, 'aaa*': {'maximum': 20}}}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "multiplesimultaneouspatternPropertiesarevalidated"
        
def test_an_invalid_due_to_one_is_invalid(db_conn):
    data = {'a': 'bar'}
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'patternProperties': {'a*': {'type': 'integer'}, 'aaa*': {'maximum': 20}}}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "multiplesimultaneouspatternPropertiesarevalidated"
        
def test_an_invalid_due_to_the_other_is_invalid(db_conn):
    data = {'aaaa': 31}
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'patternProperties': {'a*': {'type': 'integer'}, 'aaa*': {'maximum': 20}}}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "multiplesimultaneouspatternPropertiesarevalidated"
        
def test_an_invalid_due_to_both_is_invalid(db_conn):
    data = {'aaa': 'foo', 'aaaa': 31}
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'patternProperties': {'a*': {'type': 'integer'}, 'aaa*': {'maximum': 20}}}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "multiplesimultaneouspatternPropertiesarevalidated"
        
def test_non_recognized_members_are_ignored(db_conn):
    data = {'answer 1': '42'}
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'patternProperties': {'[0-9]{2,}': {'type': 'boolean'}, 'X_': {'type': 'string'}}}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "regexesarenotanchoredbydefaultandarecasesensitive"
        
def test_recognized_members_are_accounted_for(db_conn):
    data = {'a31b': None}
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'patternProperties': {'[0-9]{2,}': {'type': 'boolean'}, 'X_': {'type': 'string'}}}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "regexesarenotanchoredbydefaultandarecasesensitive"
        
def test_regexes_are_case_sensitive(db_conn):
    data = {'a_x_3': 3}
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'patternProperties': {'[0-9]{2,}': {'type': 'boolean'}, 'X_': {'type': 'string'}}}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "regexesarenotanchoredbydefaultandarecasesensitive"
        
def test_regexes_are_case_sensitive_2(db_conn):
    data = {'a_X_3': 3}
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'patternProperties': {'[0-9]{2,}': {'type': 'boolean'}, 'X_': {'type': 'string'}}}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "regexesarenotanchoredbydefaultandarecasesensitive"
        
def test_object_with_property_matching_schema_true_is_valid(db_conn):
    data = {'foo': 1}
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'patternProperties': {'f.*': True, 'b.*': False}}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "patternPropertieswithbooleanschemas"
        
def test_object_with_property_matching_schema_false_is_invalid(db_conn):
    data = {'bar': 2}
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'patternProperties': {'f.*': True, 'b.*': False}}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "patternPropertieswithbooleanschemas"
        
def test_object_with_both_properties_is_invalid(db_conn):
    data = {'foo': 1, 'bar': 2}
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'patternProperties': {'f.*': True, 'b.*': False}}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "patternPropertieswithbooleanschemas"
        
def test_object_with_a_property_matching_both_true_and_false_is_invalid(db_conn):
    data = {'foobar': 1}
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'patternProperties': {'f.*': True, 'b.*': False}}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "patternPropertieswithbooleanschemas"
        
def test_empty_object_is_valid(db_conn):
    data = {}
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'patternProperties': {'f.*': True, 'b.*': False}}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "patternPropertieswithbooleanschemas"
        
def test_allows_null_values(db_conn):
    data = {'foobar': None}
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'patternProperties': {'^.*bar$': {'type': 'null'}}}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "patternPropertieswithnullvaluedinstanceproperties"
        