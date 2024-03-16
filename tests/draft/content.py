
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


def test_a_valid_JSON_document(db_conn):
    data = '{"foo": "bar"}'
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "contentMediaType": "application/json"
}

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "validationofstringencodedcontentbasedonmediatype"
        
def test_an_invalid_JSON_document_validates_true(db_conn):
    data = '{:}'
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "contentMediaType": "application/json"
}

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "validationofstringencodedcontentbasedonmediatype"
        
def test_ignores_nonstrings(db_conn):
    data = 100
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "contentMediaType": "application/json"
}

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "validationofstringencodedcontentbasedonmediatype"
        
def test_a_valid_base64_string(db_conn):
    data = 'eyJmb28iOiAiYmFyIn0K'
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "contentEncoding": "base64"
}

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "validationofbinarystringencoding"
        
def test_an_invalid_base64_string__is_not_a_valid_character_validates_true(db_conn):
    data = 'eyJmb28iOi%iYmFyIn0K'
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "contentEncoding": "base64"
}

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "validationofbinarystringencoding"
        
def test_ignores_nonstrings(db_conn):
    data = 100
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "contentEncoding": "base64"
}

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "validationofbinarystringencoding"
        
def test_a_valid_base64encoded_JSON_document(db_conn):
    data = 'eyJmb28iOiAiYmFyIn0K'
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "contentMediaType": "application/json",
    "contentEncoding": "base64"
}

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "validationofbinaryencodedmediatypedocuments"
        
def test_a_validlyencoded_invalid_JSON_document_validates_true(db_conn):
    data = 'ezp9Cg=='
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "contentMediaType": "application/json",
    "contentEncoding": "base64"
}

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "validationofbinaryencodedmediatypedocuments"
        
def test_an_invalid_base64_string_that_is_valid_JSON_validates_true(db_conn):
    data = '{}'
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "contentMediaType": "application/json",
    "contentEncoding": "base64"
}

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "validationofbinaryencodedmediatypedocuments"
        
def test_ignores_nonstrings(db_conn):
    data = 100
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "contentMediaType": "application/json",
    "contentEncoding": "base64"
}

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "validationofbinaryencodedmediatypedocuments"
        
def test_a_valid_base64encoded_JSON_document(db_conn):
    data = 'eyJmb28iOiAiYmFyIn0K'
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "contentMediaType": "application/json",
    "contentEncoding": "base64",
    "contentSchema": {
        "type": "object",
        "required": [
            "foo"
        ],
        "properties": {
            "foo": {
                "type": "string"
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

    assert result is True, "validationofbinaryencodedmediatypedocumentswithschema"
        
def test_another_valid_base64encoded_JSON_document(db_conn):
    data = 'eyJib28iOiAyMCwgImZvbyI6ICJiYXoifQ=='
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "contentMediaType": "application/json",
    "contentEncoding": "base64",
    "contentSchema": {
        "type": "object",
        "required": [
            "foo"
        ],
        "properties": {
            "foo": {
                "type": "string"
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

    assert result is True, "validationofbinaryencodedmediatypedocumentswithschema"
        
def test_an_invalid_base64encoded_JSON_document_validates_true(db_conn):
    data = 'eyJib28iOiAyMH0='
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "contentMediaType": "application/json",
    "contentEncoding": "base64",
    "contentSchema": {
        "type": "object",
        "required": [
            "foo"
        ],
        "properties": {
            "foo": {
                "type": "string"
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

    assert result is True, "validationofbinaryencodedmediatypedocumentswithschema"
        
def test_an_empty_object_as_a_base64encoded_JSON_document_validates_true(db_conn):
    data = 'e30='
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "contentMediaType": "application/json",
    "contentEncoding": "base64",
    "contentSchema": {
        "type": "object",
        "required": [
            "foo"
        ],
        "properties": {
            "foo": {
                "type": "string"
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

    assert result is True, "validationofbinaryencodedmediatypedocumentswithschema"
        
def test_an_empty_array_as_a_base64encoded_JSON_document(db_conn):
    data = 'W10='
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "contentMediaType": "application/json",
    "contentEncoding": "base64",
    "contentSchema": {
        "type": "object",
        "required": [
            "foo"
        ],
        "properties": {
            "foo": {
                "type": "string"
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

    assert result is True, "validationofbinaryencodedmediatypedocumentswithschema"
        
def test_a_validlyencoded_invalid_JSON_document_validates_true(db_conn):
    data = 'ezp9Cg=='
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "contentMediaType": "application/json",
    "contentEncoding": "base64",
    "contentSchema": {
        "type": "object",
        "required": [
            "foo"
        ],
        "properties": {
            "foo": {
                "type": "string"
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

    assert result is True, "validationofbinaryencodedmediatypedocumentswithschema"
        
def test_an_invalid_base64_string_that_is_valid_JSON_validates_true(db_conn):
    data = '{}'
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "contentMediaType": "application/json",
    "contentEncoding": "base64",
    "contentSchema": {
        "type": "object",
        "required": [
            "foo"
        ],
        "properties": {
            "foo": {
                "type": "string"
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

    assert result is True, "validationofbinaryencodedmediatypedocumentswithschema"
        
def test_ignores_nonstrings(db_conn):
    data = 100
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "contentMediaType": "application/json",
    "contentEncoding": "base64",
    "contentSchema": {
        "type": "object",
        "required": [
            "foo"
        ],
        "properties": {
            "foo": {
                "type": "string"
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

    assert result is True, "validationofbinaryencodedmediatypedocumentswithschema"
        