
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


def test_allowed(db_conn):
    data = 'foo'
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "not": {
        "type": "integer"
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

    assert result is True, "not"
        
def test_disallowed(db_conn):
    data = 1
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "not": {
        "type": "integer"
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

    assert result is False, "not"
        
def test_valid(db_conn):
    data = 'foo'
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "not": {
        "type": [
            "integer",
            "boolean"
        ]
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

    assert result is True, "notmultipletypes"
        
def test_mismatch(db_conn):
    data = 1
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "not": {
        "type": [
            "integer",
            "boolean"
        ]
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

    assert result is False, "notmultipletypes"
        
def test_other_mismatch(db_conn):
    data = true
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "not": {
        "type": [
            "integer",
            "boolean"
        ]
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

    assert result is False, "notmultipletypes"
        
def test_match(db_conn):
    data = 1
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "not": {
        "type": "object",
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

    assert result is True, "notmorecomplexschema"
        
def test_other_match(db_conn):
    data = {"foo": 1}
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "not": {
        "type": "object",
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

    assert result is True, "notmorecomplexschema"
        
def test_mismatch(db_conn):
    data = {"foo": "bar"}
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "not": {
        "type": "object",
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

    assert result is False, "notmorecomplexschema"
        
def test_property_present(db_conn):
    data = {"foo": 1, "bar": 2}
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "properties": {
        "foo": {
            "not": {}
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

    assert result is False, "forbiddenproperty"
        
def test_property_absent(db_conn):
    data = {"bar": 1, "baz": 2}
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "properties": {
        "foo": {
            "not": {}
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

    assert result is True, "forbiddenproperty"
        
def test_number_is_invalid(db_conn):
    data = 1
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "not": {}
}

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "forbideverythingwithemptyschema"
        
def test_string_is_invalid(db_conn):
    data = 'foo'
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "not": {}
}

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "forbideverythingwithemptyschema"
        
def test_boolean_true_is_invalid(db_conn):
    data = true
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "not": {}
}

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "forbideverythingwithemptyschema"
        
def test_boolean_false_is_invalid(db_conn):
    data = false
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "not": {}
}

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "forbideverythingwithemptyschema"
        
def test_null_is_invalid(db_conn):
    data = null
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "not": {}
}

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "forbideverythingwithemptyschema"
        
def test_object_is_invalid(db_conn):
    data = {"foo": "bar"}
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "not": {}
}

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "forbideverythingwithemptyschema"
        
def test_empty_object_is_invalid(db_conn):
    data = {}
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "not": {}
}

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "forbideverythingwithemptyschema"
        
def test_array_is_invalid(db_conn):
    data = ["foo"]
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "not": {}
}

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "forbideverythingwithemptyschema"
        
def test_empty_array_is_invalid(db_conn):
    data = []
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "not": {}
}

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "forbideverythingwithemptyschema"
        
def test_number_is_invalid(db_conn):
    data = 1
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "not": true
}

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "forbideverythingwithbooleanschematrue"
        
def test_string_is_invalid(db_conn):
    data = 'foo'
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "not": true
}

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "forbideverythingwithbooleanschematrue"
        
def test_boolean_true_is_invalid(db_conn):
    data = true
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "not": true
}

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "forbideverythingwithbooleanschematrue"
        
def test_boolean_false_is_invalid(db_conn):
    data = false
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "not": true
}

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "forbideverythingwithbooleanschematrue"
        
def test_null_is_invalid(db_conn):
    data = null
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "not": true
}

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "forbideverythingwithbooleanschematrue"
        
def test_object_is_invalid(db_conn):
    data = {"foo": "bar"}
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "not": true
}

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "forbideverythingwithbooleanschematrue"
        
def test_empty_object_is_invalid(db_conn):
    data = {}
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "not": true
}

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "forbideverythingwithbooleanschematrue"
        
def test_array_is_invalid(db_conn):
    data = ["foo"]
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "not": true
}

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "forbideverythingwithbooleanschematrue"
        
def test_empty_array_is_invalid(db_conn):
    data = []
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "not": true
}

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "forbideverythingwithbooleanschematrue"
        
def test_number_is_valid(db_conn):
    data = 1
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "not": false
}

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "alloweverythingwithbooleanschemafalse"
        
def test_string_is_valid(db_conn):
    data = 'foo'
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "not": false
}

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "alloweverythingwithbooleanschemafalse"
        
def test_boolean_true_is_valid(db_conn):
    data = true
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "not": false
}

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "alloweverythingwithbooleanschemafalse"
        
def test_boolean_false_is_valid(db_conn):
    data = false
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "not": false
}

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "alloweverythingwithbooleanschemafalse"
        
def test_null_is_valid(db_conn):
    data = null
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "not": false
}

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "alloweverythingwithbooleanschemafalse"
        
def test_object_is_valid(db_conn):
    data = {"foo": "bar"}
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "not": false
}

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "alloweverythingwithbooleanschemafalse"
        
def test_empty_object_is_valid(db_conn):
    data = {}
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "not": false
}

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "alloweverythingwithbooleanschemafalse"
        
def test_array_is_valid(db_conn):
    data = ["foo"]
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "not": false
}

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "alloweverythingwithbooleanschemafalse"
        
def test_empty_array_is_valid(db_conn):
    data = []
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "not": false
}

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "alloweverythingwithbooleanschemafalse"
        
def test_any_value_is_valid(db_conn):
    data = 'foo'
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "not": {
        "not": {}
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

    assert result is True, "doublenegation"
        
def test_unevaluated_property(db_conn):
    data = {"bar": 1}
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "not": {
        "$comment": "this subschema must still produce annotations internally, even though the 'not' will ultimately discard them",
        "anyOf": [
            true,
            {
                "properties": {
                    "foo": true
                }
            }
        ],
        "unevaluatedProperties": false
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

    assert result is True, "collectannotationsinsideanotevenifcollectionisdisabled"
        
def test_annotations_are_still_collected_inside_a_not(db_conn):
    data = {"foo": 1}
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "not": {
        "$comment": "this subschema must still produce annotations internally, even though the 'not' will ultimately discard them",
        "anyOf": [
            true,
            {
                "properties": {
                    "foo": true
                }
            }
        ],
        "unevaluatedProperties": false
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

    assert result is False, "collectannotationsinsideanotevenifcollectionisdisabled"
        