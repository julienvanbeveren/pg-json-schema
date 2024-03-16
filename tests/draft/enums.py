
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


def test_one_of_the_enum_is_valid(db_conn):
    data = 1
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "enum": [
        1,
        2,
        3
    ]
}

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "simpleenumvalidation"
        
def test_something_else_is_invalid(db_conn):
    data = 4
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "enum": [
        1,
        2,
        3
    ]
}

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "simpleenumvalidation"
        
def test_one_of_the_enum_is_valid(db_conn):
    data = []
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "enum": [
        6,
        "foo",
        [],
        true,
        {
            "foo": 12
        }
    ]
}

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "heterogeneousenumvalidation"
        
def test_something_else_is_invalid(db_conn):
    data = null
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "enum": [
        6,
        "foo",
        [],
        true,
        {
            "foo": 12
        }
    ]
}

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "heterogeneousenumvalidation"
        
def test_objects_are_deep_compared(db_conn):
    data = {"foo": false}
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "enum": [
        6,
        "foo",
        [],
        true,
        {
            "foo": 12
        }
    ]
}

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "heterogeneousenumvalidation"
        
def test_valid_object_matches(db_conn):
    data = {"foo": 12}
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "enum": [
        6,
        "foo",
        [],
        true,
        {
            "foo": 12
        }
    ]
}

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "heterogeneousenumvalidation"
        
def test_extra_properties_in_object_is_invalid(db_conn):
    data = {"foo": 12, "boo": 42}
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "enum": [
        6,
        "foo",
        [],
        true,
        {
            "foo": 12
        }
    ]
}

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "heterogeneousenumvalidation"
        
def test_null_is_valid(db_conn):
    data = null
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "enum": [
        6,
        null
    ]
}

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "heterogeneousenumwithnullvalidation"
        
def test_number_is_valid(db_conn):
    data = 6
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "enum": [
        6,
        null
    ]
}

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "heterogeneousenumwithnullvalidation"
        
def test_something_else_is_invalid(db_conn):
    data = 'test'
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "enum": [
        6,
        null
    ]
}

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "heterogeneousenumwithnullvalidation"
        
def test_both_properties_are_valid(db_conn):
    data = {"foo": "foo", "bar": "bar"}
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "type": "object",
    "properties": {
        "foo": {
            "enum": [
                "foo"
            ]
        },
        "bar": {
            "enum": [
                "bar"
            ]
        }
    },
    "required": [
        "bar"
    ]
}

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "enumsinproperties"
        
def test_wrong_foo_value(db_conn):
    data = {"foo": "foot", "bar": "bar"}
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "type": "object",
    "properties": {
        "foo": {
            "enum": [
                "foo"
            ]
        },
        "bar": {
            "enum": [
                "bar"
            ]
        }
    },
    "required": [
        "bar"
    ]
}

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "enumsinproperties"
        
def test_wrong_bar_value(db_conn):
    data = {"foo": "foo", "bar": "bart"}
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "type": "object",
    "properties": {
        "foo": {
            "enum": [
                "foo"
            ]
        },
        "bar": {
            "enum": [
                "bar"
            ]
        }
    },
    "required": [
        "bar"
    ]
}

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "enumsinproperties"
        
def test_missing_optional_property_is_valid(db_conn):
    data = {"bar": "bar"}
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "type": "object",
    "properties": {
        "foo": {
            "enum": [
                "foo"
            ]
        },
        "bar": {
            "enum": [
                "bar"
            ]
        }
    },
    "required": [
        "bar"
    ]
}

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "enumsinproperties"
        
def test_missing_required_property_is_invalid(db_conn):
    data = {"foo": "foo"}
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "type": "object",
    "properties": {
        "foo": {
            "enum": [
                "foo"
            ]
        },
        "bar": {
            "enum": [
                "bar"
            ]
        }
    },
    "required": [
        "bar"
    ]
}

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "enumsinproperties"
        
def test_missing_all_properties_is_invalid(db_conn):
    data = {}
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "type": "object",
    "properties": {
        "foo": {
            "enum": [
                "foo"
            ]
        },
        "bar": {
            "enum": [
                "bar"
            ]
        }
    },
    "required": [
        "bar"
    ]
}

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "enumsinproperties"
        
def test_member_1_is_valid(db_conn):
    data = 'foobar'
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "enum": [
        "foobar",
        "foobar"
    ]
}

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "enumwithescapedcharacters"
        
def test_member_2_is_valid(db_conn):
    data = 'foobar'
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "enum": [
        "foobar",
        "foobar"
    ]
}

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "enumwithescapedcharacters"
        
def test_another_string_is_invalid(db_conn):
    data = 'abc'
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "enum": [
        "foobar",
        "foobar"
    ]
}

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "enumwithescapedcharacters"
        
def test_false_is_valid(db_conn):
    data = false
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "enum": [
        false
    ]
}

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "enumwithfalsedoesnotmatch0"
        
def test_integer_zero_is_invalid(db_conn):
    data = 0
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "enum": [
        false
    ]
}

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "enumwithfalsedoesnotmatch0"
        
def test_float_zero_is_invalid(db_conn):
    data = 0.0
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "enum": [
        false
    ]
}

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "enumwithfalsedoesnotmatch0"
        
def test_false_is_valid(db_conn):
    data = [false]
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "enum": [
        [
            false
        ]
    ]
}

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "enumwithfalsedoesnotmatch0"
        
def test_0_is_invalid(db_conn):
    data = [0]
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "enum": [
        [
            false
        ]
    ]
}

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "enumwithfalsedoesnotmatch0"
        
def test_00_is_invalid(db_conn):
    data = [0.0]
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "enum": [
        [
            false
        ]
    ]
}

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "enumwithfalsedoesnotmatch0"
        
def test_true_is_valid(db_conn):
    data = true
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "enum": [
        true
    ]
}

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "enumwithtruedoesnotmatch1"
        
def test_integer_one_is_invalid(db_conn):
    data = 1
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "enum": [
        true
    ]
}

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "enumwithtruedoesnotmatch1"
        
def test_float_one_is_invalid(db_conn):
    data = 1.0
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "enum": [
        true
    ]
}

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "enumwithtruedoesnotmatch1"
        
def test_true_is_valid(db_conn):
    data = [true]
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "enum": [
        [
            true
        ]
    ]
}

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "enumwithtruedoesnotmatch1"
        
def test_1_is_invalid(db_conn):
    data = [1]
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "enum": [
        [
            true
        ]
    ]
}

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "enumwithtruedoesnotmatch1"
        
def test_10_is_invalid(db_conn):
    data = [1.0]
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "enum": [
        [
            true
        ]
    ]
}

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "enumwithtruedoesnotmatch1"
        
def test_false_is_invalid(db_conn):
    data = false
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "enum": [
        0
    ]
}

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "enumwith0doesnotmatchfalse"
        
def test_integer_zero_is_valid(db_conn):
    data = 0
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "enum": [
        0
    ]
}

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "enumwith0doesnotmatchfalse"
        
def test_float_zero_is_valid(db_conn):
    data = 0.0
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "enum": [
        0
    ]
}

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "enumwith0doesnotmatchfalse"
        
def test_false_is_invalid(db_conn):
    data = [false]
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "enum": [
        [
            0
        ]
    ]
}

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "enumwith0doesnotmatchfalse"
        
def test_0_is_valid(db_conn):
    data = [0]
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "enum": [
        [
            0
        ]
    ]
}

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "enumwith0doesnotmatchfalse"
        
def test_00_is_valid(db_conn):
    data = [0.0]
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "enum": [
        [
            0
        ]
    ]
}

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "enumwith0doesnotmatchfalse"
        
def test_true_is_invalid(db_conn):
    data = true
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "enum": [
        1
    ]
}

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "enumwith1doesnotmatchtrue"
        
def test_integer_one_is_valid(db_conn):
    data = 1
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "enum": [
        1
    ]
}

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "enumwith1doesnotmatchtrue"
        
def test_float_one_is_valid(db_conn):
    data = 1.0
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "enum": [
        1
    ]
}

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "enumwith1doesnotmatchtrue"
        
def test_true_is_invalid(db_conn):
    data = [true]
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "enum": [
        [
            1
        ]
    ]
}

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "enumwith1doesnotmatchtrue"
        
def test_1_is_valid(db_conn):
    data = [1]
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "enum": [
        [
            1
        ]
    ]
}

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "enumwith1doesnotmatchtrue"
        
def test_10_is_valid(db_conn):
    data = [1.0]
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "enum": [
        [
            1
        ]
    ]
}

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "enumwith1doesnotmatchtrue"
        
def test_match_string_with_nul(db_conn):
    data = 'hellothere'
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "enum": [
        "hello\u0000there"
    ]
}

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "nulcharactersinstrings"
        
def test_do_not_match_string_lacking_nul(db_conn):
    data = 'hellothere'
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "enum": [
        "hello\u0000there"
    ]
}

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "nulcharactersinstrings"
        