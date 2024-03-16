
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


def test_array_with_item_matching_schema_5_is_valid(db_conn):
    data = [3, 4, 5]
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "contains": {
        "minimum": 5
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

    assert result is True, "containskeywordvalidation"
        
def test_array_with_item_matching_schema_6_is_valid(db_conn):
    data = [3, 4, 6]
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "contains": {
        "minimum": 5
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

    assert result is True, "containskeywordvalidation"
        
def test_array_with_two_items_matching_schema_5_6_is_valid(db_conn):
    data = [3, 4, 5, 6]
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "contains": {
        "minimum": 5
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

    assert result is True, "containskeywordvalidation"
        
def test_array_without_items_matching_schema_is_invalid(db_conn):
    data = [2, 3, 4]
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "contains": {
        "minimum": 5
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

    assert result is False, "containskeywordvalidation"
        
def test_empty_array_is_invalid(db_conn):
    data = []
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "contains": {
        "minimum": 5
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

    assert result is False, "containskeywordvalidation"
        
def test_not_array_is_valid(db_conn):
    data = {}
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "contains": {
        "minimum": 5
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

    assert result is True, "containskeywordvalidation"
        
def test_array_with_item_5_is_valid(db_conn):
    data = [3, 4, 5]
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "contains": {
        "const": 5
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

    assert result is True, "containskeywordwithconstkeyword"
        
def test_array_with_two_items_5_is_valid(db_conn):
    data = [3, 4, 5, 5]
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "contains": {
        "const": 5
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

    assert result is True, "containskeywordwithconstkeyword"
        
def test_array_without_item_5_is_invalid(db_conn):
    data = [1, 2, 3, 4]
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "contains": {
        "const": 5
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

    assert result is False, "containskeywordwithconstkeyword"
        
def test_any_nonempty_array_is_valid(db_conn):
    data = ["foo"]
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "contains": true
}

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "containskeywordwithbooleanschematrue"
        
def test_empty_array_is_invalid(db_conn):
    data = []
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "contains": true
}

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "containskeywordwithbooleanschematrue"
        
def test_any_nonempty_array_is_invalid(db_conn):
    data = ["foo"]
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "contains": false
}

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "containskeywordwithbooleanschemafalse"
        
def test_empty_array_is_invalid(db_conn):
    data = []
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "contains": false
}

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "containskeywordwithbooleanschemafalse"
        
def test_nonarrays_are_valid(db_conn):
    data = 'contains does not apply to strings'
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "contains": false
}

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "containskeywordwithbooleanschemafalse"
        
def test_matches_items_does_not_match_contains(db_conn):
    data = [2, 4, 8]
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "items": {
        "multipleOf": 2
    },
    "contains": {
        "multipleOf": 3
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

    assert result is False, "itemscontains"
        
def test_does_not_match_items_matches_contains(db_conn):
    data = [3, 6, 9]
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "items": {
        "multipleOf": 2
    },
    "contains": {
        "multipleOf": 3
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

    assert result is False, "itemscontains"
        
def test_matches_both_items_and_contains(db_conn):
    data = [6, 12]
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "items": {
        "multipleOf": 2
    },
    "contains": {
        "multipleOf": 3
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

    assert result is True, "itemscontains"
        
def test_matches_neither_items_nor_contains(db_conn):
    data = [1, 5]
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "items": {
        "multipleOf": 2
    },
    "contains": {
        "multipleOf": 3
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

    assert result is False, "itemscontains"
        
def test_any_nonempty_array_is_valid(db_conn):
    data = ["foo"]
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "contains": {
        "if": false,
        "else": true
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

    assert result is True, "containswithfalseifsubschema"
        
def test_empty_array_is_invalid(db_conn):
    data = []
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "contains": {
        "if": false,
        "else": true
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

    assert result is False, "containswithfalseifsubschema"
        
def test_allows_null_items(db_conn):
    data = [null]
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "contains": {
        "type": "null"
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

    assert result is True, "containswithnullinstanceelements"
        