
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


def test_applicator_vocabulary_still_works(db_conn):
    data = {'badProperty': 'this property should not exist'}
    schema = {'$id': 'https://schema/using/no/validation', '$schema': 'http://localhost:1234/draft2020-12/metaschema-no-validation.json', 'properties': {'badProperty': False, 'numberProperty': {'minimum': 10}}}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "schemathatusescustommetaschemawithwithnovalidationvocabulary"
        
def test_no_validation_valid_number(db_conn):
    data = {'numberProperty': 20}
    schema = {'$id': 'https://schema/using/no/validation', '$schema': 'http://localhost:1234/draft2020-12/metaschema-no-validation.json', 'properties': {'badProperty': False, 'numberProperty': {'minimum': 10}}}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "schemathatusescustommetaschemawithwithnovalidationvocabulary"
        
def test_no_validation_invalid_number_but_it_still_validates(db_conn):
    data = {'numberProperty': 1}
    schema = {'$id': 'https://schema/using/no/validation', '$schema': 'http://localhost:1234/draft2020-12/metaschema-no-validation.json', 'properties': {'badProperty': False, 'numberProperty': {'minimum': 10}}}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "schemathatusescustommetaschemawithwithnovalidationvocabulary"
        
def test_string_value(db_conn):
    data = 'foobar'
    schema = {'$schema': 'http://localhost:1234/draft2020-12/metaschema-optional-vocabulary.json', 'type': 'number'}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "ignoreunrecognizedoptionalvocabulary"
        
def test_number_value(db_conn):
    data = 20
    schema = {'$schema': 'http://localhost:1234/draft2020-12/metaschema-optional-vocabulary.json', 'type': 'number'}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "ignoreunrecognizedoptionalvocabulary"
        