
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


def test_one_item_valid_against_lone_minContains(db_conn):
    data = [1]
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "minContains": 1
}

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "minContainswithoutcontainsisignored"
        
def test_zero_items_still_valid_against_lone_minContains(db_conn):
    data = []
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "minContains": 1
}

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "minContainswithoutcontainsisignored"
        
def test_empty_data(db_conn):
    data = []
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "contains": {
        "const": 1
    },
    "minContains": 1
}

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "minContains1withcontains"
        
def test_no_elements_match(db_conn):
    data = [2]
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "contains": {
        "const": 1
    },
    "minContains": 1
}

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "minContains1withcontains"
        
def test_single_element_matches_valid_minContains(db_conn):
    data = [1]
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "contains": {
        "const": 1
    },
    "minContains": 1
}

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "minContains1withcontains"
        
def test_some_elements_match_valid_minContains(db_conn):
    data = [1, 2]
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "contains": {
        "const": 1
    },
    "minContains": 1
}

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "minContains1withcontains"
        
def test_all_elements_match_valid_minContains(db_conn):
    data = [1, 1]
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "contains": {
        "const": 1
    },
    "minContains": 1
}

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "minContains1withcontains"
        
def test_empty_data(db_conn):
    data = []
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "contains": {
        "const": 1
    },
    "minContains": 2
}

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "minContains2withcontains"
        
def test_all_elements_match_invalid_minContains(db_conn):
    data = [1]
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "contains": {
        "const": 1
    },
    "minContains": 2
}

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "minContains2withcontains"
        
def test_some_elements_match_invalid_minContains(db_conn):
    data = [1, 2]
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "contains": {
        "const": 1
    },
    "minContains": 2
}

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "minContains2withcontains"
        
def test_all_elements_match_valid_minContains_exactly_as_needed(db_conn):
    data = [1, 1]
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "contains": {
        "const": 1
    },
    "minContains": 2
}

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "minContains2withcontains"
        
def test_all_elements_match_valid_minContains_more_than_needed(db_conn):
    data = [1, 1, 1]
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "contains": {
        "const": 1
    },
    "minContains": 2
}

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "minContains2withcontains"
        
def test_some_elements_match_valid_minContains(db_conn):
    data = [1, 2, 1]
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "contains": {
        "const": 1
    },
    "minContains": 2
}

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "minContains2withcontains"
        
def test_one_element_matches_invalid_minContains(db_conn):
    data = [1]
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "contains": {
        "const": 1
    },
    "minContains": 2.0
}

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "minContains2withcontainswithadecimalvalue"
        
def test_both_elements_match_valid_minContains(db_conn):
    data = [1, 1]
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "contains": {
        "const": 1
    },
    "minContains": 2.0
}

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "minContains2withcontainswithadecimalvalue"
        
def test_empty_data(db_conn):
    data = []
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "contains": {
        "const": 1
    },
    "maxContains": 2,
    "minContains": 2
}

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "maxContainsminContains"
        
def test_all_elements_match_invalid_minContains(db_conn):
    data = [1]
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "contains": {
        "const": 1
    },
    "maxContains": 2,
    "minContains": 2
}

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "maxContainsminContains"
        
def test_all_elements_match_invalid_maxContains(db_conn):
    data = [1, 1, 1]
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "contains": {
        "const": 1
    },
    "maxContains": 2,
    "minContains": 2
}

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "maxContainsminContains"
        
def test_all_elements_match_valid_maxContains_and_minContains(db_conn):
    data = [1, 1]
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "contains": {
        "const": 1
    },
    "maxContains": 2,
    "minContains": 2
}

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "maxContainsminContains"
        
def test_empty_data(db_conn):
    data = []
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "contains": {
        "const": 1
    },
    "maxContains": 1,
    "minContains": 3
}

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "maxContainsminContains"
        
def test_invalid_minContains(db_conn):
    data = [1]
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "contains": {
        "const": 1
    },
    "maxContains": 1,
    "minContains": 3
}

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "maxContainsminContains"
        
def test_invalid_maxContains(db_conn):
    data = [1, 1, 1]
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "contains": {
        "const": 1
    },
    "maxContains": 1,
    "minContains": 3
}

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "maxContainsminContains"
        
def test_invalid_maxContains_and_minContains(db_conn):
    data = [1, 1]
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "contains": {
        "const": 1
    },
    "maxContains": 1,
    "minContains": 3
}

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "maxContainsminContains"
        
def test_empty_data(db_conn):
    data = []
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "contains": {
        "const": 1
    },
    "minContains": 0
}

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "minContains0"
        
def test_minContains__0_makes_contains_always_pass(db_conn):
    data = [2]
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "contains": {
        "const": 1
    },
    "minContains": 0
}

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "minContains0"
        
def test_empty_data(db_conn):
    data = []
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "contains": {
        "const": 1
    },
    "minContains": 0,
    "maxContains": 1
}

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "minContains0withmaxContains"
        
def test_not_more_than_maxContains(db_conn):
    data = [1]
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "contains": {
        "const": 1
    },
    "minContains": 0,
    "maxContains": 1
}

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "minContains0withmaxContains"
        
def test_too_many(db_conn):
    data = [1, 1]
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "contains": {
        "const": 1
    },
    "minContains": 0,
    "maxContains": 1
}

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "minContains0withmaxContains"
        