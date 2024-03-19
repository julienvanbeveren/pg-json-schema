
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


def test_same_value_is_valid(db_conn):
    data = 2
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'const': 2}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "constvalidation"
        
def test_another_value_is_invalid(db_conn):
    data = 5
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'const': 2}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "constvalidation"
        
def test_another_type_is_invalid(db_conn):
    data = 'a'
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'const': 2}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "constvalidation"
        
def test_same_object_is_valid(db_conn):
    data = {'foo': 'bar', 'baz': 'bax'}
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'const': {'foo': 'bar', 'baz': 'bax'}}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "constwithobject"
        
def test_same_object_with_different_property_order_is_valid(db_conn):
    data = {'baz': 'bax', 'foo': 'bar'}
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'const': {'foo': 'bar', 'baz': 'bax'}}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "constwithobject"
        
def test_another_object_is_invalid(db_conn):
    data = {'foo': 'bar'}
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'const': {'foo': 'bar', 'baz': 'bax'}}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "constwithobject"
        
def test_another_type_is_invalid(db_conn):
    data = [1, 2]
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'const': {'foo': 'bar', 'baz': 'bax'}}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "constwithobject"
        
def test_same_array_is_valid(db_conn):
    data = [{'foo': 'bar'}]
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'const': [{'foo': 'bar'}]}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "constwitharray"
        
def test_another_array_item_is_invalid(db_conn):
    data = [2]
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'const': [{'foo': 'bar'}]}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "constwitharray"
        
def test_array_with_additional_items_is_invalid(db_conn):
    data = [1, 2, 3]
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'const': [{'foo': 'bar'}]}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "constwitharray"
        
def test_null_is_valid(db_conn):
    data = None
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'const': None}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "constwithnull"
        
def test_not_null_is_invalid(db_conn):
    data = 0
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'const': None}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "constwithnull"
        
def test_false_is_valid(db_conn):
    data = False
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'const': False}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "constwithfalsedoesnotmatch0"
        
def test_integer_zero_is_invalid(db_conn):
    data = 0
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'const': False}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "constwithfalsedoesnotmatch0"
        
def test_float_zero_is_invalid(db_conn):
    data = 0.0
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'const': False}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "constwithfalsedoesnotmatch0"
        
def test_true_is_valid(db_conn):
    data = True
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'const': True}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "constwithtruedoesnotmatch1"
        
def test_integer_one_is_invalid(db_conn):
    data = 1
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'const': True}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "constwithtruedoesnotmatch1"
        
def test_float_one_is_invalid(db_conn):
    data = 1.0
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'const': True}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "constwithtruedoesnotmatch1"
        
def test_false_is_valid(db_conn):
    data = [False]
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'const': [False]}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "constwithfalsedoesnotmatch0"
        
def test_0_is_invalid(db_conn):
    data = [0]
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'const': [False]}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "constwithfalsedoesnotmatch0"
        
def test_00_is_invalid(db_conn):
    data = [0.0]
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'const': [False]}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "constwithfalsedoesnotmatch0"
        
def test_true_is_valid(db_conn):
    data = [True]
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'const': [True]}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "constwithtruedoesnotmatch1"
        
def test_1_is_invalid(db_conn):
    data = [1]
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'const': [True]}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "constwithtruedoesnotmatch1"
        
def test_10_is_invalid(db_conn):
    data = [1.0]
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'const': [True]}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "constwithtruedoesnotmatch1"
        
def test_a_false_is_valid(db_conn):
    data = {'a': False}
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'const': {'a': False}}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "constwithafalsedoesnotmatcha0"
        
def test_a_0_is_invalid(db_conn):
    data = {'a': 0}
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'const': {'a': False}}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "constwithafalsedoesnotmatcha0"
        
def test_a_00_is_invalid(db_conn):
    data = {'a': 0.0}
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'const': {'a': False}}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "constwithafalsedoesnotmatcha0"
        
def test_a_true_is_valid(db_conn):
    data = {'a': True}
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'const': {'a': True}}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "constwithatruedoesnotmatcha1"
        
def test_a_1_is_invalid(db_conn):
    data = {'a': 1}
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'const': {'a': True}}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "constwithatruedoesnotmatcha1"
        
def test_a_10_is_invalid(db_conn):
    data = {'a': 1.0}
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'const': {'a': True}}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "constwithatruedoesnotmatcha1"
        
def test_false_is_invalid(db_conn):
    data = False
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'const': 0}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "constwith0doesnotmatchotherzeroliketypes"
        
def test_integer_zero_is_valid(db_conn):
    data = 0
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'const': 0}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "constwith0doesnotmatchotherzeroliketypes"
        
def test_float_zero_is_valid(db_conn):
    data = 0.0
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'const': 0}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "constwith0doesnotmatchotherzeroliketypes"
        
def test_empty_object_is_invalid(db_conn):
    data = {}
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'const': 0}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "constwith0doesnotmatchotherzeroliketypes"
        
def test_empty_array_is_invalid(db_conn):
    data = []
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'const': 0}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "constwith0doesnotmatchotherzeroliketypes"
        
def test_empty_string_is_invalid(db_conn):
    data = ''
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'const': 0}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "constwith0doesnotmatchotherzeroliketypes"
        
def test_true_is_invalid(db_conn):
    data = True
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'const': 1}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "constwith1doesnotmatchtrue"
        
def test_integer_one_is_valid(db_conn):
    data = 1
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'const': 1}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "constwith1doesnotmatchtrue"
        
def test_float_one_is_valid(db_conn):
    data = 1.0
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'const': 1}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "constwith1doesnotmatchtrue"
        
def test_integer_2_is_valid(db_conn):
    data = -2
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'const': -2.0}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "constwith20matchesintegerandfloattypes"
        
def test_integer_2_is_invalid(db_conn):
    data = 2
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'const': -2.0}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "constwith20matchesintegerandfloattypes"
        
def test_float_20_is_valid(db_conn):
    data = -2.0
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'const': -2.0}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "constwith20matchesintegerandfloattypes"
        
def test_float_20_is_invalid(db_conn):
    data = 2.0
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'const': -2.0}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "constwith20matchesintegerandfloattypes"
        
def test_float_200001_is_invalid(db_conn):
    data = -2.00001
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'const': -2.0}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "constwith20matchesintegerandfloattypes"
        
def test_integer_is_valid(db_conn):
    data = 9007199254740992
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'const': 9007199254740992}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "floatandintegersareequalupto64bitrepresentationlimits"
        
def test_integer_minus_one_is_invalid(db_conn):
    data = 9007199254740991
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'const': 9007199254740992}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "floatandintegersareequalupto64bitrepresentationlimits"
        
def test_float_is_valid(db_conn):
    data = 9007199254740992.0
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'const': 9007199254740992}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "floatandintegersareequalupto64bitrepresentationlimits"
        
def test_float_minus_one_is_invalid(db_conn):
    data = 9007199254740991.0
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'const': 9007199254740992}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "floatandintegersareequalupto64bitrepresentationlimits"
        