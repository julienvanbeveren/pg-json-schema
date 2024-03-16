
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


def test_all_string_formats_ignore_integers(db_conn):
    data = 12
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'format': 'email'}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "emailformat"
        
def test_all_string_formats_ignore_floats(db_conn):
    data = 13.7
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'format': 'email'}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "emailformat"
        
def test_all_string_formats_ignore_objects(db_conn):
    data = {}
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'format': 'email'}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "emailformat"
        
def test_all_string_formats_ignore_arrays(db_conn):
    data = []
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'format': 'email'}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "emailformat"
        
def test_all_string_formats_ignore_booleans(db_conn):
    data = false
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'format': 'email'}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "emailformat"
        
def test_all_string_formats_ignore_nulls(db_conn):
    data = null
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'format': 'email'}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "emailformat"
        
def test_invalid_email_string_is_only_an_annotation_by_default(db_conn):
    data = '2962'
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'format': 'email'}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "emailformat"
        
def test_all_string_formats_ignore_integers(db_conn):
    data = 12
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'format': 'idn-email'}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "idnemailformat"
        
def test_all_string_formats_ignore_floats(db_conn):
    data = 13.7
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'format': 'idn-email'}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "idnemailformat"
        
def test_all_string_formats_ignore_objects(db_conn):
    data = {}
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'format': 'idn-email'}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "idnemailformat"
        
def test_all_string_formats_ignore_arrays(db_conn):
    data = []
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'format': 'idn-email'}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "idnemailformat"
        
def test_all_string_formats_ignore_booleans(db_conn):
    data = false
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'format': 'idn-email'}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "idnemailformat"
        
def test_all_string_formats_ignore_nulls(db_conn):
    data = null
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'format': 'idn-email'}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "idnemailformat"
        
def test_invalid_idnemail_string_is_only_an_annotation_by_default(db_conn):
    data = '2962'
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'format': 'idn-email'}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "idnemailformat"
        
def test_all_string_formats_ignore_integers(db_conn):
    data = 12
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'format': 'regex'}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "regexformat"
        
def test_all_string_formats_ignore_floats(db_conn):
    data = 13.7
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'format': 'regex'}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "regexformat"
        
def test_all_string_formats_ignore_objects(db_conn):
    data = {}
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'format': 'regex'}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "regexformat"
        
def test_all_string_formats_ignore_arrays(db_conn):
    data = []
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'format': 'regex'}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "regexformat"
        
def test_all_string_formats_ignore_booleans(db_conn):
    data = false
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'format': 'regex'}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "regexformat"
        
def test_all_string_formats_ignore_nulls(db_conn):
    data = null
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'format': 'regex'}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "regexformat"
        
def test_invalid_regex_string_is_only_an_annotation_by_default(db_conn):
    data = '^(abc]'
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'format': 'regex'}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "regexformat"
        
def test_all_string_formats_ignore_integers(db_conn):
    data = 12
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'format': 'ipv4'}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "ipv4format"
        
def test_all_string_formats_ignore_floats(db_conn):
    data = 13.7
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'format': 'ipv4'}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "ipv4format"
        
def test_all_string_formats_ignore_objects(db_conn):
    data = {}
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'format': 'ipv4'}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "ipv4format"
        
def test_all_string_formats_ignore_arrays(db_conn):
    data = []
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'format': 'ipv4'}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "ipv4format"
        
def test_all_string_formats_ignore_booleans(db_conn):
    data = false
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'format': 'ipv4'}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "ipv4format"
        
def test_all_string_formats_ignore_nulls(db_conn):
    data = null
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'format': 'ipv4'}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "ipv4format"
        
def test_invalid_ipv4_string_is_only_an_annotation_by_default(db_conn):
    data = '127.0.0.0.1'
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'format': 'ipv4'}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "ipv4format"
        
def test_all_string_formats_ignore_integers(db_conn):
    data = 12
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'format': 'ipv6'}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "ipv6format"
        
def test_all_string_formats_ignore_floats(db_conn):
    data = 13.7
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'format': 'ipv6'}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "ipv6format"
        
def test_all_string_formats_ignore_objects(db_conn):
    data = {}
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'format': 'ipv6'}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "ipv6format"
        
def test_all_string_formats_ignore_arrays(db_conn):
    data = []
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'format': 'ipv6'}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "ipv6format"
        
def test_all_string_formats_ignore_booleans(db_conn):
    data = false
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'format': 'ipv6'}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "ipv6format"
        
def test_all_string_formats_ignore_nulls(db_conn):
    data = null
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'format': 'ipv6'}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "ipv6format"
        
def test_invalid_ipv6_string_is_only_an_annotation_by_default(db_conn):
    data = '12345::'
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'format': 'ipv6'}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "ipv6format"
        
def test_all_string_formats_ignore_integers(db_conn):
    data = 12
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'format': 'idn-hostname'}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "idnhostnameformat"
        
def test_all_string_formats_ignore_floats(db_conn):
    data = 13.7
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'format': 'idn-hostname'}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "idnhostnameformat"
        
def test_all_string_formats_ignore_objects(db_conn):
    data = {}
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'format': 'idn-hostname'}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "idnhostnameformat"
        
def test_all_string_formats_ignore_arrays(db_conn):
    data = []
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'format': 'idn-hostname'}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "idnhostnameformat"
        
def test_all_string_formats_ignore_booleans(db_conn):
    data = false
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'format': 'idn-hostname'}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "idnhostnameformat"
        
def test_all_string_formats_ignore_nulls(db_conn):
    data = null
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'format': 'idn-hostname'}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "idnhostnameformat"
        
def test_invalid_idnhostname_string_is_only_an_annotation_by_default(db_conn):
    data = '〮실례.테스트'
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'format': 'idn-hostname'}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "idnhostnameformat"
        
def test_all_string_formats_ignore_integers(db_conn):
    data = 12
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'format': 'hostname'}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "hostnameformat"
        
def test_all_string_formats_ignore_floats(db_conn):
    data = 13.7
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'format': 'hostname'}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "hostnameformat"
        
def test_all_string_formats_ignore_objects(db_conn):
    data = {}
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'format': 'hostname'}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "hostnameformat"
        
def test_all_string_formats_ignore_arrays(db_conn):
    data = []
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'format': 'hostname'}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "hostnameformat"
        
def test_all_string_formats_ignore_booleans(db_conn):
    data = false
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'format': 'hostname'}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "hostnameformat"
        
def test_all_string_formats_ignore_nulls(db_conn):
    data = null
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'format': 'hostname'}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "hostnameformat"
        
def test_invalid_hostname_string_is_only_an_annotation_by_default(db_conn):
    data = '-a-host-name-that-starts-with--'
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'format': 'hostname'}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "hostnameformat"
        
def test_all_string_formats_ignore_integers(db_conn):
    data = 12
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'format': 'date'}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "dateformat"
        
def test_all_string_formats_ignore_floats(db_conn):
    data = 13.7
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'format': 'date'}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "dateformat"
        
def test_all_string_formats_ignore_objects(db_conn):
    data = {}
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'format': 'date'}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "dateformat"
        
def test_all_string_formats_ignore_arrays(db_conn):
    data = []
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'format': 'date'}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "dateformat"
        
def test_all_string_formats_ignore_booleans(db_conn):
    data = false
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'format': 'date'}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "dateformat"
        
def test_all_string_formats_ignore_nulls(db_conn):
    data = null
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'format': 'date'}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "dateformat"
        
def test_invalid_date_string_is_only_an_annotation_by_default(db_conn):
    data = '06/19/1963'
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'format': 'date'}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "dateformat"
        
def test_all_string_formats_ignore_integers(db_conn):
    data = 12
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'format': 'date-time'}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "datetimeformat"
        
def test_all_string_formats_ignore_floats(db_conn):
    data = 13.7
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'format': 'date-time'}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "datetimeformat"
        
def test_all_string_formats_ignore_objects(db_conn):
    data = {}
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'format': 'date-time'}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "datetimeformat"
        
def test_all_string_formats_ignore_arrays(db_conn):
    data = []
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'format': 'date-time'}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "datetimeformat"
        
def test_all_string_formats_ignore_booleans(db_conn):
    data = false
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'format': 'date-time'}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "datetimeformat"
        
def test_all_string_formats_ignore_nulls(db_conn):
    data = null
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'format': 'date-time'}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "datetimeformat"
        
def test_invalid_datetime_string_is_only_an_annotation_by_default(db_conn):
    data = '1990-02-31T15:59:60.123-08:00'
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'format': 'date-time'}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "datetimeformat"
        
def test_all_string_formats_ignore_integers(db_conn):
    data = 12
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'format': 'time'}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "timeformat"
        
def test_all_string_formats_ignore_floats(db_conn):
    data = 13.7
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'format': 'time'}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "timeformat"
        
def test_all_string_formats_ignore_objects(db_conn):
    data = {}
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'format': 'time'}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "timeformat"
        
def test_all_string_formats_ignore_arrays(db_conn):
    data = []
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'format': 'time'}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "timeformat"
        
def test_all_string_formats_ignore_booleans(db_conn):
    data = false
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'format': 'time'}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "timeformat"
        
def test_all_string_formats_ignore_nulls(db_conn):
    data = null
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'format': 'time'}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "timeformat"
        
def test_invalid_time_string_is_only_an_annotation_by_default(db_conn):
    data = '08:30:06 PST'
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'format': 'time'}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "timeformat"
        
def test_all_string_formats_ignore_integers(db_conn):
    data = 12
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'format': 'json-pointer'}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "jsonpointerformat"
        
def test_all_string_formats_ignore_floats(db_conn):
    data = 13.7
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'format': 'json-pointer'}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "jsonpointerformat"
        
def test_all_string_formats_ignore_objects(db_conn):
    data = {}
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'format': 'json-pointer'}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "jsonpointerformat"
        
def test_all_string_formats_ignore_arrays(db_conn):
    data = []
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'format': 'json-pointer'}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "jsonpointerformat"
        
def test_all_string_formats_ignore_booleans(db_conn):
    data = false
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'format': 'json-pointer'}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "jsonpointerformat"
        
def test_all_string_formats_ignore_nulls(db_conn):
    data = null
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'format': 'json-pointer'}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "jsonpointerformat"
        
def test_invalid_jsonpointer_string_is_only_an_annotation_by_default(db_conn):
    data = '/foo/bar~'
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'format': 'json-pointer'}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "jsonpointerformat"
        
def test_all_string_formats_ignore_integers(db_conn):
    data = 12
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'format': 'relative-json-pointer'}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "relativejsonpointerformat"
        
def test_all_string_formats_ignore_floats(db_conn):
    data = 13.7
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'format': 'relative-json-pointer'}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "relativejsonpointerformat"
        
def test_all_string_formats_ignore_objects(db_conn):
    data = {}
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'format': 'relative-json-pointer'}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "relativejsonpointerformat"
        
def test_all_string_formats_ignore_arrays(db_conn):
    data = []
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'format': 'relative-json-pointer'}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "relativejsonpointerformat"
        
def test_all_string_formats_ignore_booleans(db_conn):
    data = false
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'format': 'relative-json-pointer'}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "relativejsonpointerformat"
        
def test_all_string_formats_ignore_nulls(db_conn):
    data = null
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'format': 'relative-json-pointer'}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "relativejsonpointerformat"
        
def test_invalid_relativejsonpointer_string_is_only_an_annotation_by_default(db_conn):
    data = '/foo/bar'
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'format': 'relative-json-pointer'}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "relativejsonpointerformat"
        
def test_all_string_formats_ignore_integers(db_conn):
    data = 12
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'format': 'iri'}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "iriformat"
        
def test_all_string_formats_ignore_floats(db_conn):
    data = 13.7
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'format': 'iri'}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "iriformat"
        
def test_all_string_formats_ignore_objects(db_conn):
    data = {}
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'format': 'iri'}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "iriformat"
        
def test_all_string_formats_ignore_arrays(db_conn):
    data = []
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'format': 'iri'}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "iriformat"
        
def test_all_string_formats_ignore_booleans(db_conn):
    data = false
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'format': 'iri'}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "iriformat"
        
def test_all_string_formats_ignore_nulls(db_conn):
    data = null
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'format': 'iri'}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "iriformat"
        
def test_invalid_iri_string_is_only_an_annotation_by_default(db_conn):
    data = 'http://2001:0db8:85a3:0000:0000:8a2e:0370:7334'
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'format': 'iri'}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "iriformat"
        
def test_all_string_formats_ignore_integers(db_conn):
    data = 12
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'format': 'iri-reference'}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "irireferenceformat"
        
def test_all_string_formats_ignore_floats(db_conn):
    data = 13.7
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'format': 'iri-reference'}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "irireferenceformat"
        
def test_all_string_formats_ignore_objects(db_conn):
    data = {}
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'format': 'iri-reference'}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "irireferenceformat"
        
def test_all_string_formats_ignore_arrays(db_conn):
    data = []
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'format': 'iri-reference'}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "irireferenceformat"
        
def test_all_string_formats_ignore_booleans(db_conn):
    data = false
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'format': 'iri-reference'}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "irireferenceformat"
        
def test_all_string_formats_ignore_nulls(db_conn):
    data = null
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'format': 'iri-reference'}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "irireferenceformat"
        
def test_invalid_irireference_string_is_only_an_annotation_by_default(db_conn):
    data = '\\WINDOWS\filëßåré'
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'format': 'iri-reference'}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "irireferenceformat"
        
def test_all_string_formats_ignore_integers(db_conn):
    data = 12
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'format': 'uri'}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "uriformat"
        
def test_all_string_formats_ignore_floats(db_conn):
    data = 13.7
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'format': 'uri'}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "uriformat"
        
def test_all_string_formats_ignore_objects(db_conn):
    data = {}
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'format': 'uri'}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "uriformat"
        
def test_all_string_formats_ignore_arrays(db_conn):
    data = []
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'format': 'uri'}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "uriformat"
        
def test_all_string_formats_ignore_booleans(db_conn):
    data = false
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'format': 'uri'}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "uriformat"
        
def test_all_string_formats_ignore_nulls(db_conn):
    data = null
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'format': 'uri'}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "uriformat"
        
def test_invalid_uri_string_is_only_an_annotation_by_default(db_conn):
    data = '//foo.bar/?baz=qux#quux'
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'format': 'uri'}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "uriformat"
        
def test_all_string_formats_ignore_integers(db_conn):
    data = 12
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'format': 'uri-reference'}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "urireferenceformat"
        
def test_all_string_formats_ignore_floats(db_conn):
    data = 13.7
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'format': 'uri-reference'}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "urireferenceformat"
        
def test_all_string_formats_ignore_objects(db_conn):
    data = {}
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'format': 'uri-reference'}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "urireferenceformat"
        
def test_all_string_formats_ignore_arrays(db_conn):
    data = []
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'format': 'uri-reference'}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "urireferenceformat"
        
def test_all_string_formats_ignore_booleans(db_conn):
    data = false
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'format': 'uri-reference'}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "urireferenceformat"
        
def test_all_string_formats_ignore_nulls(db_conn):
    data = null
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'format': 'uri-reference'}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "urireferenceformat"
        
def test_invalid_urireference_string_is_only_an_annotation_by_default(db_conn):
    data = '\\WINDOWS\fileshare'
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'format': 'uri-reference'}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "urireferenceformat"
        
def test_all_string_formats_ignore_integers(db_conn):
    data = 12
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'format': 'uri-template'}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "uritemplateformat"
        
def test_all_string_formats_ignore_floats(db_conn):
    data = 13.7
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'format': 'uri-template'}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "uritemplateformat"
        
def test_all_string_formats_ignore_objects(db_conn):
    data = {}
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'format': 'uri-template'}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "uritemplateformat"
        
def test_all_string_formats_ignore_arrays(db_conn):
    data = []
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'format': 'uri-template'}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "uritemplateformat"
        
def test_all_string_formats_ignore_booleans(db_conn):
    data = false
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'format': 'uri-template'}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "uritemplateformat"
        
def test_all_string_formats_ignore_nulls(db_conn):
    data = null
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'format': 'uri-template'}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "uritemplateformat"
        
def test_invalid_uritemplate_string_is_only_an_annotation_by_default(db_conn):
    data = 'http://example.com/dictionary/{term:1}/{term'
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'format': 'uri-template'}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "uritemplateformat"
        
def test_all_string_formats_ignore_integers(db_conn):
    data = 12
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'format': 'uuid'}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "uuidformat"
        
def test_all_string_formats_ignore_floats(db_conn):
    data = 13.7
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'format': 'uuid'}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "uuidformat"
        
def test_all_string_formats_ignore_objects(db_conn):
    data = {}
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'format': 'uuid'}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "uuidformat"
        
def test_all_string_formats_ignore_arrays(db_conn):
    data = []
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'format': 'uuid'}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "uuidformat"
        
def test_all_string_formats_ignore_booleans(db_conn):
    data = false
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'format': 'uuid'}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "uuidformat"
        
def test_all_string_formats_ignore_nulls(db_conn):
    data = null
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'format': 'uuid'}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "uuidformat"
        
def test_invalid_uuid_string_is_only_an_annotation_by_default(db_conn):
    data = '2eb8aa08-aa98-11ea-b4aa-73b441d1638'
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'format': 'uuid'}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "uuidformat"
        
def test_all_string_formats_ignore_integers(db_conn):
    data = 12
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'format': 'duration'}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "durationformat"
        
def test_all_string_formats_ignore_floats(db_conn):
    data = 13.7
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'format': 'duration'}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "durationformat"
        
def test_all_string_formats_ignore_objects(db_conn):
    data = {}
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'format': 'duration'}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "durationformat"
        
def test_all_string_formats_ignore_arrays(db_conn):
    data = []
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'format': 'duration'}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "durationformat"
        
def test_all_string_formats_ignore_booleans(db_conn):
    data = false
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'format': 'duration'}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "durationformat"
        
def test_all_string_formats_ignore_nulls(db_conn):
    data = null
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'format': 'duration'}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "durationformat"
        
def test_invalid_duration_string_is_only_an_annotation_by_default(db_conn):
    data = 'PT1D'
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema', 'format': 'duration'}

    data_str = json.dumps(data)
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "durationformat"
        