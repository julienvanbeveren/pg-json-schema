
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


def test_neither(db_conn):
    data = {}
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "dependentRequired": {
        "bar": [
            "foo"
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

    assert result is True, "singledependency"
        
def test_nondependant(db_conn):
    data = {"foo": 1}
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "dependentRequired": {
        "bar": [
            "foo"
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

    assert result is True, "singledependency"
        
def test_with_dependency(db_conn):
    data = {"foo": 1, "bar": 2}
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "dependentRequired": {
        "bar": [
            "foo"
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

    assert result is True, "singledependency"
        
def test_missing_dependency(db_conn):
    data = {"bar": 2}
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "dependentRequired": {
        "bar": [
            "foo"
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

    assert result is False, "singledependency"
        
def test_ignores_arrays(db_conn):
    data = ["bar"]
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "dependentRequired": {
        "bar": [
            "foo"
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

    assert result is True, "singledependency"
        
def test_ignores_strings(db_conn):
    data = 'foobar'
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "dependentRequired": {
        "bar": [
            "foo"
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

    assert result is True, "singledependency"
        
def test_ignores_other_nonobjects(db_conn):
    data = 12
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "dependentRequired": {
        "bar": [
            "foo"
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

    assert result is True, "singledependency"
        
def test_empty_object(db_conn):
    data = {}
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "dependentRequired": {
        "bar": []
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

    assert result is True, "emptydependents"
        
def test_object_with_one_property(db_conn):
    data = {"bar": 2}
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "dependentRequired": {
        "bar": []
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

    assert result is True, "emptydependents"
        
def test_nonobject_is_valid(db_conn):
    data = 1
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "dependentRequired": {
        "bar": []
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

    assert result is True, "emptydependents"
        
def test_neither(db_conn):
    data = {}
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "dependentRequired": {
        "quux": [
            "foo",
            "bar"
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

    assert result is True, "multipledependentsrequired"
        
def test_nondependants(db_conn):
    data = {"foo": 1, "bar": 2}
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "dependentRequired": {
        "quux": [
            "foo",
            "bar"
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

    assert result is True, "multipledependentsrequired"
        
def test_with_dependencies(db_conn):
    data = {"foo": 1, "bar": 2, "quux": 3}
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "dependentRequired": {
        "quux": [
            "foo",
            "bar"
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

    assert result is True, "multipledependentsrequired"
        
def test_missing_dependency(db_conn):
    data = {"foo": 1, "quux": 2}
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "dependentRequired": {
        "quux": [
            "foo",
            "bar"
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

    assert result is False, "multipledependentsrequired"
        
def test_missing_other_dependency(db_conn):
    data = {"bar": 1, "quux": 2}
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "dependentRequired": {
        "quux": [
            "foo",
            "bar"
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

    assert result is False, "multipledependentsrequired"
        
def test_missing_both_dependencies(db_conn):
    data = {"quux": 1}
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "dependentRequired": {
        "quux": [
            "foo",
            "bar"
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

    assert result is False, "multipledependentsrequired"
        
def test_CRLF(db_conn):
    data = {"foo\nbar": 1, "foo\rbar": 2}
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "dependentRequired": {
        "foo\nbar": [
            "foo\rbar"
        ],
        "foo\"bar": [
            "foo'bar"
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

    assert result is True, "dependencieswithescapedcharacters"
        
def test_quoted_quotes(db_conn):
    data = {"foo'bar": 1, "foo\"bar": 2}
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "dependentRequired": {
        "foo\nbar": [
            "foo\rbar"
        ],
        "foo\"bar": [
            "foo'bar"
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

    assert result is True, "dependencieswithescapedcharacters"
        
def test_CRLF_missing_dependent(db_conn):
    data = {"foo\nbar": 1, "foo": 2}
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "dependentRequired": {
        "foo\nbar": [
            "foo\rbar"
        ],
        "foo\"bar": [
            "foo'bar"
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

    assert result is False, "dependencieswithescapedcharacters"
        
def test_quoted_quotes_missing_dependent(db_conn):
    data = {"foo\"bar": 2}
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "dependentRequired": {
        "foo\nbar": [
            "foo\rbar"
        ],
        "foo\"bar": [
            "foo'bar"
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

    assert result is False, "dependencieswithescapedcharacters"
        