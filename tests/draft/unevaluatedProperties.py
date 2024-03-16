
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


def test_with_no_unevaluated_properties(db_conn):
    data = {}
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "type": "object",
    "unevaluatedProperties": true
}

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "unevaluatedPropertiestrue"
        
def test_with_unevaluated_properties(db_conn):
    data = {"foo": "foo"}
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "type": "object",
    "unevaluatedProperties": true
}

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "unevaluatedPropertiestrue"
        
def test_with_no_unevaluated_properties(db_conn):
    data = {}
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "type": "object",
    "unevaluatedProperties": {
        "type": "string",
        "minLength": 3
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

    assert result is True, "unevaluatedPropertiesschema"
        
def test_with_valid_unevaluated_properties(db_conn):
    data = {"foo": "foo"}
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "type": "object",
    "unevaluatedProperties": {
        "type": "string",
        "minLength": 3
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

    assert result is True, "unevaluatedPropertiesschema"
        
def test_with_invalid_unevaluated_properties(db_conn):
    data = {"foo": "fo"}
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "type": "object",
    "unevaluatedProperties": {
        "type": "string",
        "minLength": 3
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

    assert result is False, "unevaluatedPropertiesschema"
        
def test_with_no_unevaluated_properties(db_conn):
    data = {}
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "type": "object",
    "unevaluatedProperties": false
}

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "unevaluatedPropertiesfalse"
        
def test_with_unevaluated_properties(db_conn):
    data = {"foo": "foo"}
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "type": "object",
    "unevaluatedProperties": false
}

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "unevaluatedPropertiesfalse"
        
def test_with_no_unevaluated_properties(db_conn):
    data = {"foo": "foo"}
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "type": "object",
    "properties": {
        "foo": {
            "type": "string"
        }
    },
    "unevaluatedProperties": false
}

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "unevaluatedPropertieswithadjacentproperties"
        
def test_with_unevaluated_properties(db_conn):
    data = {"foo": "foo", "bar": "bar"}
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "type": "object",
    "properties": {
        "foo": {
            "type": "string"
        }
    },
    "unevaluatedProperties": false
}

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "unevaluatedPropertieswithadjacentproperties"
        
def test_with_no_unevaluated_properties(db_conn):
    data = {"foo": "foo"}
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "type": "object",
    "patternProperties": {
        "^foo": {
            "type": "string"
        }
    },
    "unevaluatedProperties": false
}

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "unevaluatedPropertieswithadjacentpatternProperties"
        
def test_with_unevaluated_properties(db_conn):
    data = {"foo": "foo", "bar": "bar"}
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "type": "object",
    "patternProperties": {
        "^foo": {
            "type": "string"
        }
    },
    "unevaluatedProperties": false
}

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "unevaluatedPropertieswithadjacentpatternProperties"
        
def test_with_no_additional_properties(db_conn):
    data = {"foo": "foo"}
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "type": "object",
    "properties": {
        "foo": {
            "type": "string"
        }
    },
    "additionalProperties": true,
    "unevaluatedProperties": false
}

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "unevaluatedPropertieswithadjacentadditionalProperties"
        
def test_with_additional_properties(db_conn):
    data = {"foo": "foo", "bar": "bar"}
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "type": "object",
    "properties": {
        "foo": {
            "type": "string"
        }
    },
    "additionalProperties": true,
    "unevaluatedProperties": false
}

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "unevaluatedPropertieswithadjacentadditionalProperties"
        
def test_with_no_additional_properties(db_conn):
    data = {"foo": "foo", "bar": "bar"}
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "type": "object",
    "properties": {
        "foo": {
            "type": "string"
        }
    },
    "allOf": [
        {
            "properties": {
                "bar": {
                    "type": "string"
                }
            }
        }
    ],
    "unevaluatedProperties": false
}

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "unevaluatedPropertieswithnestedproperties"
        
def test_with_additional_properties(db_conn):
    data = {"foo": "foo", "bar": "bar", "baz": "baz"}
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "type": "object",
    "properties": {
        "foo": {
            "type": "string"
        }
    },
    "allOf": [
        {
            "properties": {
                "bar": {
                    "type": "string"
                }
            }
        }
    ],
    "unevaluatedProperties": false
}

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "unevaluatedPropertieswithnestedproperties"
        
def test_with_no_additional_properties(db_conn):
    data = {"foo": "foo", "bar": "bar"}
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "type": "object",
    "properties": {
        "foo": {
            "type": "string"
        }
    },
    "allOf": [
        {
            "patternProperties": {
                "^bar": {
                    "type": "string"
                }
            }
        }
    ],
    "unevaluatedProperties": false
}

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "unevaluatedPropertieswithnestedpatternProperties"
        
def test_with_additional_properties(db_conn):
    data = {"foo": "foo", "bar": "bar", "baz": "baz"}
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "type": "object",
    "properties": {
        "foo": {
            "type": "string"
        }
    },
    "allOf": [
        {
            "patternProperties": {
                "^bar": {
                    "type": "string"
                }
            }
        }
    ],
    "unevaluatedProperties": false
}

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "unevaluatedPropertieswithnestedpatternProperties"
        
def test_with_no_additional_properties(db_conn):
    data = {"foo": "foo"}
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "type": "object",
    "properties": {
        "foo": {
            "type": "string"
        }
    },
    "allOf": [
        {
            "additionalProperties": true
        }
    ],
    "unevaluatedProperties": false
}

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "unevaluatedPropertieswithnestedadditionalProperties"
        
def test_with_additional_properties(db_conn):
    data = {"foo": "foo", "bar": "bar"}
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "type": "object",
    "properties": {
        "foo": {
            "type": "string"
        }
    },
    "allOf": [
        {
            "additionalProperties": true
        }
    ],
    "unevaluatedProperties": false
}

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "unevaluatedPropertieswithnestedadditionalProperties"
        
def test_with_no_nested_unevaluated_properties(db_conn):
    data = {"foo": "foo"}
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "type": "object",
    "properties": {
        "foo": {
            "type": "string"
        }
    },
    "allOf": [
        {
            "unevaluatedProperties": true
        }
    ],
    "unevaluatedProperties": {
        "type": "string",
        "maxLength": 2
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

    assert result is True, "unevaluatedPropertieswithnestedunevaluatedProperties"
        
def test_with_nested_unevaluated_properties(db_conn):
    data = {"foo": "foo", "bar": "bar"}
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "type": "object",
    "properties": {
        "foo": {
            "type": "string"
        }
    },
    "allOf": [
        {
            "unevaluatedProperties": true
        }
    ],
    "unevaluatedProperties": {
        "type": "string",
        "maxLength": 2
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

    assert result is True, "unevaluatedPropertieswithnestedunevaluatedProperties"
        
def test_when_one_matches_and_has_no_unevaluated_properties(db_conn):
    data = {"foo": "foo", "bar": "bar"}
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "type": "object",
    "properties": {
        "foo": {
            "type": "string"
        }
    },
    "anyOf": [
        {
            "properties": {
                "bar": {
                    "const": "bar"
                }
            },
            "required": [
                "bar"
            ]
        },
        {
            "properties": {
                "baz": {
                    "const": "baz"
                }
            },
            "required": [
                "baz"
            ]
        },
        {
            "properties": {
                "quux": {
                    "const": "quux"
                }
            },
            "required": [
                "quux"
            ]
        }
    ],
    "unevaluatedProperties": false
}

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "unevaluatedPropertieswithanyOf"
        
def test_when_one_matches_and_has_unevaluated_properties(db_conn):
    data = {"foo": "foo", "bar": "bar", "baz": "not-baz"}
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "type": "object",
    "properties": {
        "foo": {
            "type": "string"
        }
    },
    "anyOf": [
        {
            "properties": {
                "bar": {
                    "const": "bar"
                }
            },
            "required": [
                "bar"
            ]
        },
        {
            "properties": {
                "baz": {
                    "const": "baz"
                }
            },
            "required": [
                "baz"
            ]
        },
        {
            "properties": {
                "quux": {
                    "const": "quux"
                }
            },
            "required": [
                "quux"
            ]
        }
    ],
    "unevaluatedProperties": false
}

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "unevaluatedPropertieswithanyOf"
        
def test_when_two_match_and_has_no_unevaluated_properties(db_conn):
    data = {"foo": "foo", "bar": "bar", "baz": "baz"}
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "type": "object",
    "properties": {
        "foo": {
            "type": "string"
        }
    },
    "anyOf": [
        {
            "properties": {
                "bar": {
                    "const": "bar"
                }
            },
            "required": [
                "bar"
            ]
        },
        {
            "properties": {
                "baz": {
                    "const": "baz"
                }
            },
            "required": [
                "baz"
            ]
        },
        {
            "properties": {
                "quux": {
                    "const": "quux"
                }
            },
            "required": [
                "quux"
            ]
        }
    ],
    "unevaluatedProperties": false
}

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "unevaluatedPropertieswithanyOf"
        
def test_when_two_match_and_has_unevaluated_properties(db_conn):
    data = {"foo": "foo", "bar": "bar", "baz": "baz", "quux": "not-quux"}
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "type": "object",
    "properties": {
        "foo": {
            "type": "string"
        }
    },
    "anyOf": [
        {
            "properties": {
                "bar": {
                    "const": "bar"
                }
            },
            "required": [
                "bar"
            ]
        },
        {
            "properties": {
                "baz": {
                    "const": "baz"
                }
            },
            "required": [
                "baz"
            ]
        },
        {
            "properties": {
                "quux": {
                    "const": "quux"
                }
            },
            "required": [
                "quux"
            ]
        }
    ],
    "unevaluatedProperties": false
}

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "unevaluatedPropertieswithanyOf"
        
def test_with_no_unevaluated_properties(db_conn):
    data = {"foo": "foo", "bar": "bar"}
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "type": "object",
    "properties": {
        "foo": {
            "type": "string"
        }
    },
    "oneOf": [
        {
            "properties": {
                "bar": {
                    "const": "bar"
                }
            },
            "required": [
                "bar"
            ]
        },
        {
            "properties": {
                "baz": {
                    "const": "baz"
                }
            },
            "required": [
                "baz"
            ]
        }
    ],
    "unevaluatedProperties": false
}

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "unevaluatedPropertieswithoneOf"
        
def test_with_unevaluated_properties(db_conn):
    data = {"foo": "foo", "bar": "bar", "quux": "quux"}
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "type": "object",
    "properties": {
        "foo": {
            "type": "string"
        }
    },
    "oneOf": [
        {
            "properties": {
                "bar": {
                    "const": "bar"
                }
            },
            "required": [
                "bar"
            ]
        },
        {
            "properties": {
                "baz": {
                    "const": "baz"
                }
            },
            "required": [
                "baz"
            ]
        }
    ],
    "unevaluatedProperties": false
}

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "unevaluatedPropertieswithoneOf"
        
def test_with_unevaluated_properties(db_conn):
    data = {"foo": "foo", "bar": "bar"}
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "type": "object",
    "properties": {
        "foo": {
            "type": "string"
        }
    },
    "not": {
        "not": {
            "properties": {
                "bar": {
                    "const": "bar"
                }
            },
            "required": [
                "bar"
            ]
        }
    },
    "unevaluatedProperties": false
}

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "unevaluatedPropertieswithnot"
        
def test_when_if_is_true_and_has_no_unevaluated_properties(db_conn):
    data = {"foo": "then", "bar": "bar"}
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "type": "object",
    "if": {
        "properties": {
            "foo": {
                "const": "then"
            }
        },
        "required": [
            "foo"
        ]
    },
    "then": {
        "properties": {
            "bar": {
                "type": "string"
            }
        },
        "required": [
            "bar"
        ]
    },
    "else": {
        "properties": {
            "baz": {
                "type": "string"
            }
        },
        "required": [
            "baz"
        ]
    },
    "unevaluatedProperties": false
}

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "unevaluatedPropertieswithifthenelse"
        
def test_when_if_is_true_and_has_unevaluated_properties(db_conn):
    data = {"foo": "then", "bar": "bar", "baz": "baz"}
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "type": "object",
    "if": {
        "properties": {
            "foo": {
                "const": "then"
            }
        },
        "required": [
            "foo"
        ]
    },
    "then": {
        "properties": {
            "bar": {
                "type": "string"
            }
        },
        "required": [
            "bar"
        ]
    },
    "else": {
        "properties": {
            "baz": {
                "type": "string"
            }
        },
        "required": [
            "baz"
        ]
    },
    "unevaluatedProperties": false
}

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "unevaluatedPropertieswithifthenelse"
        
def test_when_if_is_false_and_has_no_unevaluated_properties(db_conn):
    data = {"baz": "baz"}
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "type": "object",
    "if": {
        "properties": {
            "foo": {
                "const": "then"
            }
        },
        "required": [
            "foo"
        ]
    },
    "then": {
        "properties": {
            "bar": {
                "type": "string"
            }
        },
        "required": [
            "bar"
        ]
    },
    "else": {
        "properties": {
            "baz": {
                "type": "string"
            }
        },
        "required": [
            "baz"
        ]
    },
    "unevaluatedProperties": false
}

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "unevaluatedPropertieswithifthenelse"
        
def test_when_if_is_false_and_has_unevaluated_properties(db_conn):
    data = {"foo": "else", "baz": "baz"}
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "type": "object",
    "if": {
        "properties": {
            "foo": {
                "const": "then"
            }
        },
        "required": [
            "foo"
        ]
    },
    "then": {
        "properties": {
            "bar": {
                "type": "string"
            }
        },
        "required": [
            "bar"
        ]
    },
    "else": {
        "properties": {
            "baz": {
                "type": "string"
            }
        },
        "required": [
            "baz"
        ]
    },
    "unevaluatedProperties": false
}

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "unevaluatedPropertieswithifthenelse"
        
def test_when_if_is_true_and_has_no_unevaluated_properties(db_conn):
    data = {"foo": "then", "bar": "bar"}
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "type": "object",
    "if": {
        "properties": {
            "foo": {
                "const": "then"
            }
        },
        "required": [
            "foo"
        ]
    },
    "else": {
        "properties": {
            "baz": {
                "type": "string"
            }
        },
        "required": [
            "baz"
        ]
    },
    "unevaluatedProperties": false
}

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "unevaluatedPropertieswithifthenelsethennotdefined"
        
def test_when_if_is_true_and_has_unevaluated_properties(db_conn):
    data = {"foo": "then", "bar": "bar", "baz": "baz"}
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "type": "object",
    "if": {
        "properties": {
            "foo": {
                "const": "then"
            }
        },
        "required": [
            "foo"
        ]
    },
    "else": {
        "properties": {
            "baz": {
                "type": "string"
            }
        },
        "required": [
            "baz"
        ]
    },
    "unevaluatedProperties": false
}

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "unevaluatedPropertieswithifthenelsethennotdefined"
        
def test_when_if_is_false_and_has_no_unevaluated_properties(db_conn):
    data = {"baz": "baz"}
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "type": "object",
    "if": {
        "properties": {
            "foo": {
                "const": "then"
            }
        },
        "required": [
            "foo"
        ]
    },
    "else": {
        "properties": {
            "baz": {
                "type": "string"
            }
        },
        "required": [
            "baz"
        ]
    },
    "unevaluatedProperties": false
}

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "unevaluatedPropertieswithifthenelsethennotdefined"
        
def test_when_if_is_false_and_has_unevaluated_properties(db_conn):
    data = {"foo": "else", "baz": "baz"}
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "type": "object",
    "if": {
        "properties": {
            "foo": {
                "const": "then"
            }
        },
        "required": [
            "foo"
        ]
    },
    "else": {
        "properties": {
            "baz": {
                "type": "string"
            }
        },
        "required": [
            "baz"
        ]
    },
    "unevaluatedProperties": false
}

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "unevaluatedPropertieswithifthenelsethennotdefined"
        
def test_when_if_is_true_and_has_no_unevaluated_properties(db_conn):
    data = {"foo": "then", "bar": "bar"}
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "type": "object",
    "if": {
        "properties": {
            "foo": {
                "const": "then"
            }
        },
        "required": [
            "foo"
        ]
    },
    "then": {
        "properties": {
            "bar": {
                "type": "string"
            }
        },
        "required": [
            "bar"
        ]
    },
    "unevaluatedProperties": false
}

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "unevaluatedPropertieswithifthenelseelsenotdefined"
        
def test_when_if_is_true_and_has_unevaluated_properties(db_conn):
    data = {"foo": "then", "bar": "bar", "baz": "baz"}
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "type": "object",
    "if": {
        "properties": {
            "foo": {
                "const": "then"
            }
        },
        "required": [
            "foo"
        ]
    },
    "then": {
        "properties": {
            "bar": {
                "type": "string"
            }
        },
        "required": [
            "bar"
        ]
    },
    "unevaluatedProperties": false
}

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "unevaluatedPropertieswithifthenelseelsenotdefined"
        
def test_when_if_is_false_and_has_no_unevaluated_properties(db_conn):
    data = {"baz": "baz"}
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "type": "object",
    "if": {
        "properties": {
            "foo": {
                "const": "then"
            }
        },
        "required": [
            "foo"
        ]
    },
    "then": {
        "properties": {
            "bar": {
                "type": "string"
            }
        },
        "required": [
            "bar"
        ]
    },
    "unevaluatedProperties": false
}

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "unevaluatedPropertieswithifthenelseelsenotdefined"
        
def test_when_if_is_false_and_has_unevaluated_properties(db_conn):
    data = {"foo": "else", "baz": "baz"}
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "type": "object",
    "if": {
        "properties": {
            "foo": {
                "const": "then"
            }
        },
        "required": [
            "foo"
        ]
    },
    "then": {
        "properties": {
            "bar": {
                "type": "string"
            }
        },
        "required": [
            "bar"
        ]
    },
    "unevaluatedProperties": false
}

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "unevaluatedPropertieswithifthenelseelsenotdefined"
        
def test_with_no_unevaluated_properties(db_conn):
    data = {"foo": "foo", "bar": "bar"}
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "type": "object",
    "properties": {
        "foo": {
            "type": "string"
        }
    },
    "dependentSchemas": {
        "foo": {
            "properties": {
                "bar": {
                    "const": "bar"
                }
            },
            "required": [
                "bar"
            ]
        }
    },
    "unevaluatedProperties": false
}

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "unevaluatedPropertieswithdependentSchemas"
        
def test_with_unevaluated_properties(db_conn):
    data = {"bar": "bar"}
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "type": "object",
    "properties": {
        "foo": {
            "type": "string"
        }
    },
    "dependentSchemas": {
        "foo": {
            "properties": {
                "bar": {
                    "const": "bar"
                }
            },
            "required": [
                "bar"
            ]
        }
    },
    "unevaluatedProperties": false
}

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "unevaluatedPropertieswithdependentSchemas"
        
def test_with_no_unevaluated_properties(db_conn):
    data = {"foo": "foo"}
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "type": "object",
    "properties": {
        "foo": {
            "type": "string"
        }
    },
    "allOf": [
        true
    ],
    "unevaluatedProperties": false
}

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "unevaluatedPropertieswithbooleanschemas"
        
def test_with_unevaluated_properties(db_conn):
    data = {"bar": "bar"}
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "type": "object",
    "properties": {
        "foo": {
            "type": "string"
        }
    },
    "allOf": [
        true
    ],
    "unevaluatedProperties": false
}

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "unevaluatedPropertieswithbooleanschemas"
        
def test_with_no_unevaluated_properties(db_conn):
    data = {"foo": "foo", "bar": "bar"}
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "type": "object",
    "$ref": "#/$defs/bar",
    "properties": {
        "foo": {
            "type": "string"
        }
    },
    "unevaluatedProperties": false,
    "$defs": {
        "bar": {
            "properties": {
                "bar": {
                    "type": "string"
                }
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

    assert result is True, "unevaluatedPropertieswithref"
        
def test_with_unevaluated_properties(db_conn):
    data = {"foo": "foo", "bar": "bar", "baz": "baz"}
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "type": "object",
    "$ref": "#/$defs/bar",
    "properties": {
        "foo": {
            "type": "string"
        }
    },
    "unevaluatedProperties": false,
    "$defs": {
        "bar": {
            "properties": {
                "bar": {
                    "type": "string"
                }
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

    assert result is False, "unevaluatedPropertieswithref"
        
def test_with_no_unevaluated_properties(db_conn):
    data = {"foo": "foo", "bar": "bar"}
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "type": "object",
    "unevaluatedProperties": false,
    "properties": {
        "foo": {
            "type": "string"
        }
    },
    "$ref": "#/$defs/bar",
    "$defs": {
        "bar": {
            "properties": {
                "bar": {
                    "type": "string"
                }
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

    assert result is True, "unevaluatedPropertiesbeforeref"
        
def test_with_unevaluated_properties(db_conn):
    data = {"foo": "foo", "bar": "bar", "baz": "baz"}
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "type": "object",
    "unevaluatedProperties": false,
    "properties": {
        "foo": {
            "type": "string"
        }
    },
    "$ref": "#/$defs/bar",
    "$defs": {
        "bar": {
            "properties": {
                "bar": {
                    "type": "string"
                }
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

    assert result is False, "unevaluatedPropertiesbeforeref"
        
def test_with_no_unevaluated_properties(db_conn):
    data = {"foo": "foo", "bar": "bar"}
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "$id": "https://example.com/unevaluated-properties-with-dynamic-ref/derived",
    "$ref": "./baseSchema",
    "$defs": {
        "derived": {
            "$dynamicAnchor": "addons",
            "properties": {
                "bar": {
                    "type": "string"
                }
            }
        },
        "baseSchema": {
            "$id": "./baseSchema",
            "$comment": "unevaluatedProperties comes first so it's more likely to catch bugs with implementations that are sensitive to keyword ordering",
            "unevaluatedProperties": false,
            "type": "object",
            "properties": {
                "foo": {
                    "type": "string"
                }
            },
            "$dynamicRef": "#addons",
            "$defs": {
                "defaultAddons": {
                    "$comment": "Needed to satisfy the bookending requirement",
                    "$dynamicAnchor": "addons"
                }
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

    assert result is True, "unevaluatedPropertieswithdynamicRef"
        
def test_with_unevaluated_properties(db_conn):
    data = {"foo": "foo", "bar": "bar", "baz": "baz"}
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "$id": "https://example.com/unevaluated-properties-with-dynamic-ref/derived",
    "$ref": "./baseSchema",
    "$defs": {
        "derived": {
            "$dynamicAnchor": "addons",
            "properties": {
                "bar": {
                    "type": "string"
                }
            }
        },
        "baseSchema": {
            "$id": "./baseSchema",
            "$comment": "unevaluatedProperties comes first so it's more likely to catch bugs with implementations that are sensitive to keyword ordering",
            "unevaluatedProperties": false,
            "type": "object",
            "properties": {
                "foo": {
                    "type": "string"
                }
            },
            "$dynamicRef": "#addons",
            "$defs": {
                "defaultAddons": {
                    "$comment": "Needed to satisfy the bookending requirement",
                    "$dynamicAnchor": "addons"
                }
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

    assert result is False, "unevaluatedPropertieswithdynamicRef"
        
def test_always_fails(db_conn):
    data = {"foo": 1}
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "allOf": [
        {
            "properties": {
                "foo": true
            }
        },
        {
            "unevaluatedProperties": false
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

    assert result is False, "unevaluatedPropertiescantseeinsidecousins"
        
def test_always_fails(db_conn):
    data = {"foo": 1}
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "allOf": [
        {
            "unevaluatedProperties": false
        },
        {
            "properties": {
                "foo": true
            }
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

    assert result is False, "unevaluatedPropertiescantseeinsidecousinsreverseorder"
        
def test_with_no_nested_unevaluated_properties(db_conn):
    data = {"foo": "foo"}
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "type": "object",
    "properties": {
        "foo": {
            "type": "string"
        }
    },
    "allOf": [
        {
            "unevaluatedProperties": true
        }
    ],
    "unevaluatedProperties": false
}

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "nestedunevaluatedPropertiesouterfalseinnertruepropertiesoutside"
        
def test_with_nested_unevaluated_properties(db_conn):
    data = {"foo": "foo", "bar": "bar"}
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "type": "object",
    "properties": {
        "foo": {
            "type": "string"
        }
    },
    "allOf": [
        {
            "unevaluatedProperties": true
        }
    ],
    "unevaluatedProperties": false
}

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "nestedunevaluatedPropertiesouterfalseinnertruepropertiesoutside"
        
def test_with_no_nested_unevaluated_properties(db_conn):
    data = {"foo": "foo"}
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "type": "object",
    "allOf": [
        {
            "properties": {
                "foo": {
                    "type": "string"
                }
            },
            "unevaluatedProperties": true
        }
    ],
    "unevaluatedProperties": false
}

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "nestedunevaluatedPropertiesouterfalseinnertruepropertiesinside"
        
def test_with_nested_unevaluated_properties(db_conn):
    data = {"foo": "foo", "bar": "bar"}
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "type": "object",
    "allOf": [
        {
            "properties": {
                "foo": {
                    "type": "string"
                }
            },
            "unevaluatedProperties": true
        }
    ],
    "unevaluatedProperties": false
}

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "nestedunevaluatedPropertiesouterfalseinnertruepropertiesinside"
        
def test_with_no_nested_unevaluated_properties(db_conn):
    data = {"foo": "foo"}
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "type": "object",
    "properties": {
        "foo": {
            "type": "string"
        }
    },
    "allOf": [
        {
            "unevaluatedProperties": false
        }
    ],
    "unevaluatedProperties": true
}

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "nestedunevaluatedPropertiesoutertrueinnerfalsepropertiesoutside"
        
def test_with_nested_unevaluated_properties(db_conn):
    data = {"foo": "foo", "bar": "bar"}
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "type": "object",
    "properties": {
        "foo": {
            "type": "string"
        }
    },
    "allOf": [
        {
            "unevaluatedProperties": false
        }
    ],
    "unevaluatedProperties": true
}

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "nestedunevaluatedPropertiesoutertrueinnerfalsepropertiesoutside"
        
def test_with_no_nested_unevaluated_properties(db_conn):
    data = {"foo": "foo"}
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "type": "object",
    "allOf": [
        {
            "properties": {
                "foo": {
                    "type": "string"
                }
            },
            "unevaluatedProperties": false
        }
    ],
    "unevaluatedProperties": true
}

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "nestedunevaluatedPropertiesoutertrueinnerfalsepropertiesinside"
        
def test_with_nested_unevaluated_properties(db_conn):
    data = {"foo": "foo", "bar": "bar"}
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "type": "object",
    "allOf": [
        {
            "properties": {
                "foo": {
                    "type": "string"
                }
            },
            "unevaluatedProperties": false
        }
    ],
    "unevaluatedProperties": true
}

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "nestedunevaluatedPropertiesoutertrueinnerfalsepropertiesinside"
        
def test_with_no_nested_unevaluated_properties(db_conn):
    data = {"foo": "foo"}
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "type": "object",
    "allOf": [
        {
            "properties": {
                "foo": {
                    "type": "string"
                }
            },
            "unevaluatedProperties": true
        },
        {
            "unevaluatedProperties": false
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

    assert result is False, "cousinunevaluatedPropertiestrueandfalsetruewithproperties"
        
def test_with_nested_unevaluated_properties(db_conn):
    data = {"foo": "foo", "bar": "bar"}
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "type": "object",
    "allOf": [
        {
            "properties": {
                "foo": {
                    "type": "string"
                }
            },
            "unevaluatedProperties": true
        },
        {
            "unevaluatedProperties": false
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

    assert result is False, "cousinunevaluatedPropertiestrueandfalsetruewithproperties"
        
def test_with_no_nested_unevaluated_properties(db_conn):
    data = {"foo": "foo"}
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "type": "object",
    "allOf": [
        {
            "unevaluatedProperties": true
        },
        {
            "properties": {
                "foo": {
                    "type": "string"
                }
            },
            "unevaluatedProperties": false
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

    assert result is True, "cousinunevaluatedPropertiestrueandfalsefalsewithproperties"
        
def test_with_nested_unevaluated_properties(db_conn):
    data = {"foo": "foo", "bar": "bar"}
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "type": "object",
    "allOf": [
        {
            "unevaluatedProperties": true
        },
        {
            "properties": {
                "foo": {
                    "type": "string"
                }
            },
            "unevaluatedProperties": false
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

    assert result is False, "cousinunevaluatedPropertiestrueandfalsefalsewithproperties"
        
def test_no_extra_properties(db_conn):
    data = {"foo": {"bar": "test"}}
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "type": "object",
    "properties": {
        "foo": {
            "type": "object",
            "properties": {
                "bar": {
                    "type": "string"
                }
            },
            "unevaluatedProperties": false
        }
    },
    "anyOf": [
        {
            "properties": {
                "foo": {
                    "properties": {
                        "faz": {
                            "type": "string"
                        }
                    }
                }
            }
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

    assert result is True, "propertyisevaluatedinanuncleschematounevaluatedProperties"
        
def test_uncle_keyword_evaluation_is_not_significant(db_conn):
    data = {"foo": {"bar": "test", "faz": "test"}}
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "type": "object",
    "properties": {
        "foo": {
            "type": "object",
            "properties": {
                "bar": {
                    "type": "string"
                }
            },
            "unevaluatedProperties": false
        }
    },
    "anyOf": [
        {
            "properties": {
                "foo": {
                    "properties": {
                        "faz": {
                            "type": "string"
                        }
                    }
                }
            }
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

    assert result is False, "propertyisevaluatedinanuncleschematounevaluatedProperties"
        
def test_base_case_both_properties_present(db_conn):
    data = {"foo": 1, "bar": 1}
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "type": "object",
    "allOf": [
        {
            "properties": {
                "foo": true
            },
            "unevaluatedProperties": false
        }
    ],
    "anyOf": [
        {
            "properties": {
                "bar": true
            }
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

    assert result is False, "inplaceapplicatorsiblingsallOfhasunevaluated"
        
def test_in_place_applicator_siblings_bar_is_missing(db_conn):
    data = {"foo": 1}
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "type": "object",
    "allOf": [
        {
            "properties": {
                "foo": true
            },
            "unevaluatedProperties": false
        }
    ],
    "anyOf": [
        {
            "properties": {
                "bar": true
            }
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

    assert result is True, "inplaceapplicatorsiblingsallOfhasunevaluated"
        
def test_in_place_applicator_siblings_foo_is_missing(db_conn):
    data = {"bar": 1}
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "type": "object",
    "allOf": [
        {
            "properties": {
                "foo": true
            },
            "unevaluatedProperties": false
        }
    ],
    "anyOf": [
        {
            "properties": {
                "bar": true
            }
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

    assert result is False, "inplaceapplicatorsiblingsallOfhasunevaluated"
        
def test_base_case_both_properties_present(db_conn):
    data = {"foo": 1, "bar": 1}
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "type": "object",
    "allOf": [
        {
            "properties": {
                "foo": true
            }
        }
    ],
    "anyOf": [
        {
            "properties": {
                "bar": true
            },
            "unevaluatedProperties": false
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

    assert result is False, "inplaceapplicatorsiblingsanyOfhasunevaluated"
        
def test_in_place_applicator_siblings_bar_is_missing(db_conn):
    data = {"foo": 1}
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "type": "object",
    "allOf": [
        {
            "properties": {
                "foo": true
            }
        }
    ],
    "anyOf": [
        {
            "properties": {
                "bar": true
            },
            "unevaluatedProperties": false
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

    assert result is False, "inplaceapplicatorsiblingsanyOfhasunevaluated"
        
def test_in_place_applicator_siblings_foo_is_missing(db_conn):
    data = {"bar": 1}
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "type": "object",
    "allOf": [
        {
            "properties": {
                "foo": true
            }
        }
    ],
    "anyOf": [
        {
            "properties": {
                "bar": true
            },
            "unevaluatedProperties": false
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

    assert result is True, "inplaceapplicatorsiblingsanyOfhasunevaluated"
        
def test_Empty_is_valid(db_conn):
    data = {}
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "type": "object",
    "properties": {
        "x": {
            "$ref": "#"
        }
    },
    "unevaluatedProperties": false
}

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "unevaluatedPropertiessinglecyclicref"
        
def test_Single_is_valid(db_conn):
    data = {"x": {}}
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "type": "object",
    "properties": {
        "x": {
            "$ref": "#"
        }
    },
    "unevaluatedProperties": false
}

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "unevaluatedPropertiessinglecyclicref"
        
def test_Unevaluated_on_1st_level_is_invalid(db_conn):
    data = {"x": {}, "y": {}}
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "type": "object",
    "properties": {
        "x": {
            "$ref": "#"
        }
    },
    "unevaluatedProperties": false
}

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "unevaluatedPropertiessinglecyclicref"
        
def test_Nested_is_valid(db_conn):
    data = {"x": {"x": {}}}
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "type": "object",
    "properties": {
        "x": {
            "$ref": "#"
        }
    },
    "unevaluatedProperties": false
}

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "unevaluatedPropertiessinglecyclicref"
        
def test_Unevaluated_on_2nd_level_is_invalid(db_conn):
    data = {"x": {"x": {}, "y": {}}}
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "type": "object",
    "properties": {
        "x": {
            "$ref": "#"
        }
    },
    "unevaluatedProperties": false
}

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "unevaluatedPropertiessinglecyclicref"
        
def test_Deep_nested_is_valid(db_conn):
    data = {"x": {"x": {"x": {}}}}
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "type": "object",
    "properties": {
        "x": {
            "$ref": "#"
        }
    },
    "unevaluatedProperties": false
}

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "unevaluatedPropertiessinglecyclicref"
        
def test_Unevaluated_on_3rd_level_is_invalid(db_conn):
    data = {"x": {"x": {"x": {}, "y": {}}}}
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "type": "object",
    "properties": {
        "x": {
            "$ref": "#"
        }
    },
    "unevaluatedProperties": false
}

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "unevaluatedPropertiessinglecyclicref"
        
def test_Empty_is_invalid_no_x_or_y(db_conn):
    data = {}
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "$defs": {
        "one": {
            "properties": {
                "a": true
            }
        },
        "two": {
            "required": [
                "x"
            ],
            "properties": {
                "x": true
            }
        }
    },
    "allOf": [
        {
            "$ref": "#/$defs/one"
        },
        {
            "properties": {
                "b": true
            }
        },
        {
            "oneOf": [
                {
                    "$ref": "#/$defs/two"
                },
                {
                    "required": [
                        "y"
                    ],
                    "properties": {
                        "y": true
                    }
                }
            ]
        }
    ],
    "unevaluatedProperties": false
}

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "unevaluatedPropertiesrefinsideallOfoneOf"
        
def test_a_and_b_are_invalid_no_x_or_y(db_conn):
    data = {"a": 1, "b": 1}
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "$defs": {
        "one": {
            "properties": {
                "a": true
            }
        },
        "two": {
            "required": [
                "x"
            ],
            "properties": {
                "x": true
            }
        }
    },
    "allOf": [
        {
            "$ref": "#/$defs/one"
        },
        {
            "properties": {
                "b": true
            }
        },
        {
            "oneOf": [
                {
                    "$ref": "#/$defs/two"
                },
                {
                    "required": [
                        "y"
                    ],
                    "properties": {
                        "y": true
                    }
                }
            ]
        }
    ],
    "unevaluatedProperties": false
}

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "unevaluatedPropertiesrefinsideallOfoneOf"
        
def test_x_and_y_are_invalid(db_conn):
    data = {"x": 1, "y": 1}
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "$defs": {
        "one": {
            "properties": {
                "a": true
            }
        },
        "two": {
            "required": [
                "x"
            ],
            "properties": {
                "x": true
            }
        }
    },
    "allOf": [
        {
            "$ref": "#/$defs/one"
        },
        {
            "properties": {
                "b": true
            }
        },
        {
            "oneOf": [
                {
                    "$ref": "#/$defs/two"
                },
                {
                    "required": [
                        "y"
                    ],
                    "properties": {
                        "y": true
                    }
                }
            ]
        }
    ],
    "unevaluatedProperties": false
}

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "unevaluatedPropertiesrefinsideallOfoneOf"
        
def test_a_and_x_are_valid(db_conn):
    data = {"a": 1, "x": 1}
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "$defs": {
        "one": {
            "properties": {
                "a": true
            }
        },
        "two": {
            "required": [
                "x"
            ],
            "properties": {
                "x": true
            }
        }
    },
    "allOf": [
        {
            "$ref": "#/$defs/one"
        },
        {
            "properties": {
                "b": true
            }
        },
        {
            "oneOf": [
                {
                    "$ref": "#/$defs/two"
                },
                {
                    "required": [
                        "y"
                    ],
                    "properties": {
                        "y": true
                    }
                }
            ]
        }
    ],
    "unevaluatedProperties": false
}

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "unevaluatedPropertiesrefinsideallOfoneOf"
        
def test_a_and_y_are_valid(db_conn):
    data = {"a": 1, "y": 1}
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "$defs": {
        "one": {
            "properties": {
                "a": true
            }
        },
        "two": {
            "required": [
                "x"
            ],
            "properties": {
                "x": true
            }
        }
    },
    "allOf": [
        {
            "$ref": "#/$defs/one"
        },
        {
            "properties": {
                "b": true
            }
        },
        {
            "oneOf": [
                {
                    "$ref": "#/$defs/two"
                },
                {
                    "required": [
                        "y"
                    ],
                    "properties": {
                        "y": true
                    }
                }
            ]
        }
    ],
    "unevaluatedProperties": false
}

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "unevaluatedPropertiesrefinsideallOfoneOf"
        
def test_a_and_b_and_x_are_valid(db_conn):
    data = {"a": 1, "b": 1, "x": 1}
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "$defs": {
        "one": {
            "properties": {
                "a": true
            }
        },
        "two": {
            "required": [
                "x"
            ],
            "properties": {
                "x": true
            }
        }
    },
    "allOf": [
        {
            "$ref": "#/$defs/one"
        },
        {
            "properties": {
                "b": true
            }
        },
        {
            "oneOf": [
                {
                    "$ref": "#/$defs/two"
                },
                {
                    "required": [
                        "y"
                    ],
                    "properties": {
                        "y": true
                    }
                }
            ]
        }
    ],
    "unevaluatedProperties": false
}

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "unevaluatedPropertiesrefinsideallOfoneOf"
        
def test_a_and_b_and_y_are_valid(db_conn):
    data = {"a": 1, "b": 1, "y": 1}
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "$defs": {
        "one": {
            "properties": {
                "a": true
            }
        },
        "two": {
            "required": [
                "x"
            ],
            "properties": {
                "x": true
            }
        }
    },
    "allOf": [
        {
            "$ref": "#/$defs/one"
        },
        {
            "properties": {
                "b": true
            }
        },
        {
            "oneOf": [
                {
                    "$ref": "#/$defs/two"
                },
                {
                    "required": [
                        "y"
                    ],
                    "properties": {
                        "y": true
                    }
                }
            ]
        }
    ],
    "unevaluatedProperties": false
}

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "unevaluatedPropertiesrefinsideallOfoneOf"
        
def test_a_and_b_and_x_and_y_are_invalid(db_conn):
    data = {"a": 1, "b": 1, "x": 1, "y": 1}
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "$defs": {
        "one": {
            "properties": {
                "a": true
            }
        },
        "two": {
            "required": [
                "x"
            ],
            "properties": {
                "x": true
            }
        }
    },
    "allOf": [
        {
            "$ref": "#/$defs/one"
        },
        {
            "properties": {
                "b": true
            }
        },
        {
            "oneOf": [
                {
                    "$ref": "#/$defs/two"
                },
                {
                    "required": [
                        "y"
                    ],
                    "properties": {
                        "y": true
                    }
                }
            ]
        }
    ],
    "unevaluatedProperties": false
}

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "unevaluatedPropertiesrefinsideallOfoneOf"
        
def test_Empty_is_invalid(db_conn):
    data = {}
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "$defs": {
        "one": {
            "oneOf": [
                {
                    "$ref": "#/$defs/two"
                },
                {
                    "required": [
                        "b"
                    ],
                    "properties": {
                        "b": true
                    }
                },
                {
                    "required": [
                        "xx"
                    ],
                    "patternProperties": {
                        "x": true
                    }
                },
                {
                    "required": [
                        "all"
                    ],
                    "unevaluatedProperties": true
                }
            ]
        },
        "two": {
            "oneOf": [
                {
                    "required": [
                        "c"
                    ],
                    "properties": {
                        "c": true
                    }
                },
                {
                    "required": [
                        "d"
                    ],
                    "properties": {
                        "d": true
                    }
                }
            ]
        }
    },
    "oneOf": [
        {
            "$ref": "#/$defs/one"
        },
        {
            "required": [
                "a"
            ],
            "properties": {
                "a": true
            }
        }
    ],
    "unevaluatedProperties": false
}

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "dynamicevalationinsidenestedrefs"
        
def test_a_is_valid(db_conn):
    data = {"a": 1}
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "$defs": {
        "one": {
            "oneOf": [
                {
                    "$ref": "#/$defs/two"
                },
                {
                    "required": [
                        "b"
                    ],
                    "properties": {
                        "b": true
                    }
                },
                {
                    "required": [
                        "xx"
                    ],
                    "patternProperties": {
                        "x": true
                    }
                },
                {
                    "required": [
                        "all"
                    ],
                    "unevaluatedProperties": true
                }
            ]
        },
        "two": {
            "oneOf": [
                {
                    "required": [
                        "c"
                    ],
                    "properties": {
                        "c": true
                    }
                },
                {
                    "required": [
                        "d"
                    ],
                    "properties": {
                        "d": true
                    }
                }
            ]
        }
    },
    "oneOf": [
        {
            "$ref": "#/$defs/one"
        },
        {
            "required": [
                "a"
            ],
            "properties": {
                "a": true
            }
        }
    ],
    "unevaluatedProperties": false
}

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "dynamicevalationinsidenestedrefs"
        
def test_b_is_valid(db_conn):
    data = {"b": 1}
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "$defs": {
        "one": {
            "oneOf": [
                {
                    "$ref": "#/$defs/two"
                },
                {
                    "required": [
                        "b"
                    ],
                    "properties": {
                        "b": true
                    }
                },
                {
                    "required": [
                        "xx"
                    ],
                    "patternProperties": {
                        "x": true
                    }
                },
                {
                    "required": [
                        "all"
                    ],
                    "unevaluatedProperties": true
                }
            ]
        },
        "two": {
            "oneOf": [
                {
                    "required": [
                        "c"
                    ],
                    "properties": {
                        "c": true
                    }
                },
                {
                    "required": [
                        "d"
                    ],
                    "properties": {
                        "d": true
                    }
                }
            ]
        }
    },
    "oneOf": [
        {
            "$ref": "#/$defs/one"
        },
        {
            "required": [
                "a"
            ],
            "properties": {
                "a": true
            }
        }
    ],
    "unevaluatedProperties": false
}

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "dynamicevalationinsidenestedrefs"
        
def test_c_is_valid(db_conn):
    data = {"c": 1}
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "$defs": {
        "one": {
            "oneOf": [
                {
                    "$ref": "#/$defs/two"
                },
                {
                    "required": [
                        "b"
                    ],
                    "properties": {
                        "b": true
                    }
                },
                {
                    "required": [
                        "xx"
                    ],
                    "patternProperties": {
                        "x": true
                    }
                },
                {
                    "required": [
                        "all"
                    ],
                    "unevaluatedProperties": true
                }
            ]
        },
        "two": {
            "oneOf": [
                {
                    "required": [
                        "c"
                    ],
                    "properties": {
                        "c": true
                    }
                },
                {
                    "required": [
                        "d"
                    ],
                    "properties": {
                        "d": true
                    }
                }
            ]
        }
    },
    "oneOf": [
        {
            "$ref": "#/$defs/one"
        },
        {
            "required": [
                "a"
            ],
            "properties": {
                "a": true
            }
        }
    ],
    "unevaluatedProperties": false
}

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "dynamicevalationinsidenestedrefs"
        
def test_d_is_valid(db_conn):
    data = {"d": 1}
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "$defs": {
        "one": {
            "oneOf": [
                {
                    "$ref": "#/$defs/two"
                },
                {
                    "required": [
                        "b"
                    ],
                    "properties": {
                        "b": true
                    }
                },
                {
                    "required": [
                        "xx"
                    ],
                    "patternProperties": {
                        "x": true
                    }
                },
                {
                    "required": [
                        "all"
                    ],
                    "unevaluatedProperties": true
                }
            ]
        },
        "two": {
            "oneOf": [
                {
                    "required": [
                        "c"
                    ],
                    "properties": {
                        "c": true
                    }
                },
                {
                    "required": [
                        "d"
                    ],
                    "properties": {
                        "d": true
                    }
                }
            ]
        }
    },
    "oneOf": [
        {
            "$ref": "#/$defs/one"
        },
        {
            "required": [
                "a"
            ],
            "properties": {
                "a": true
            }
        }
    ],
    "unevaluatedProperties": false
}

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "dynamicevalationinsidenestedrefs"
        
def test_a__b_is_invalid(db_conn):
    data = {"a": 1, "b": 1}
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "$defs": {
        "one": {
            "oneOf": [
                {
                    "$ref": "#/$defs/two"
                },
                {
                    "required": [
                        "b"
                    ],
                    "properties": {
                        "b": true
                    }
                },
                {
                    "required": [
                        "xx"
                    ],
                    "patternProperties": {
                        "x": true
                    }
                },
                {
                    "required": [
                        "all"
                    ],
                    "unevaluatedProperties": true
                }
            ]
        },
        "two": {
            "oneOf": [
                {
                    "required": [
                        "c"
                    ],
                    "properties": {
                        "c": true
                    }
                },
                {
                    "required": [
                        "d"
                    ],
                    "properties": {
                        "d": true
                    }
                }
            ]
        }
    },
    "oneOf": [
        {
            "$ref": "#/$defs/one"
        },
        {
            "required": [
                "a"
            ],
            "properties": {
                "a": true
            }
        }
    ],
    "unevaluatedProperties": false
}

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "dynamicevalationinsidenestedrefs"
        
def test_a__c_is_invalid(db_conn):
    data = {"a": 1, "c": 1}
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "$defs": {
        "one": {
            "oneOf": [
                {
                    "$ref": "#/$defs/two"
                },
                {
                    "required": [
                        "b"
                    ],
                    "properties": {
                        "b": true
                    }
                },
                {
                    "required": [
                        "xx"
                    ],
                    "patternProperties": {
                        "x": true
                    }
                },
                {
                    "required": [
                        "all"
                    ],
                    "unevaluatedProperties": true
                }
            ]
        },
        "two": {
            "oneOf": [
                {
                    "required": [
                        "c"
                    ],
                    "properties": {
                        "c": true
                    }
                },
                {
                    "required": [
                        "d"
                    ],
                    "properties": {
                        "d": true
                    }
                }
            ]
        }
    },
    "oneOf": [
        {
            "$ref": "#/$defs/one"
        },
        {
            "required": [
                "a"
            ],
            "properties": {
                "a": true
            }
        }
    ],
    "unevaluatedProperties": false
}

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "dynamicevalationinsidenestedrefs"
        
def test_a__d_is_invalid(db_conn):
    data = {"a": 1, "d": 1}
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "$defs": {
        "one": {
            "oneOf": [
                {
                    "$ref": "#/$defs/two"
                },
                {
                    "required": [
                        "b"
                    ],
                    "properties": {
                        "b": true
                    }
                },
                {
                    "required": [
                        "xx"
                    ],
                    "patternProperties": {
                        "x": true
                    }
                },
                {
                    "required": [
                        "all"
                    ],
                    "unevaluatedProperties": true
                }
            ]
        },
        "two": {
            "oneOf": [
                {
                    "required": [
                        "c"
                    ],
                    "properties": {
                        "c": true
                    }
                },
                {
                    "required": [
                        "d"
                    ],
                    "properties": {
                        "d": true
                    }
                }
            ]
        }
    },
    "oneOf": [
        {
            "$ref": "#/$defs/one"
        },
        {
            "required": [
                "a"
            ],
            "properties": {
                "a": true
            }
        }
    ],
    "unevaluatedProperties": false
}

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "dynamicevalationinsidenestedrefs"
        
def test_b__c_is_invalid(db_conn):
    data = {"b": 1, "c": 1}
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "$defs": {
        "one": {
            "oneOf": [
                {
                    "$ref": "#/$defs/two"
                },
                {
                    "required": [
                        "b"
                    ],
                    "properties": {
                        "b": true
                    }
                },
                {
                    "required": [
                        "xx"
                    ],
                    "patternProperties": {
                        "x": true
                    }
                },
                {
                    "required": [
                        "all"
                    ],
                    "unevaluatedProperties": true
                }
            ]
        },
        "two": {
            "oneOf": [
                {
                    "required": [
                        "c"
                    ],
                    "properties": {
                        "c": true
                    }
                },
                {
                    "required": [
                        "d"
                    ],
                    "properties": {
                        "d": true
                    }
                }
            ]
        }
    },
    "oneOf": [
        {
            "$ref": "#/$defs/one"
        },
        {
            "required": [
                "a"
            ],
            "properties": {
                "a": true
            }
        }
    ],
    "unevaluatedProperties": false
}

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "dynamicevalationinsidenestedrefs"
        
def test_b__d_is_invalid(db_conn):
    data = {"b": 1, "d": 1}
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "$defs": {
        "one": {
            "oneOf": [
                {
                    "$ref": "#/$defs/two"
                },
                {
                    "required": [
                        "b"
                    ],
                    "properties": {
                        "b": true
                    }
                },
                {
                    "required": [
                        "xx"
                    ],
                    "patternProperties": {
                        "x": true
                    }
                },
                {
                    "required": [
                        "all"
                    ],
                    "unevaluatedProperties": true
                }
            ]
        },
        "two": {
            "oneOf": [
                {
                    "required": [
                        "c"
                    ],
                    "properties": {
                        "c": true
                    }
                },
                {
                    "required": [
                        "d"
                    ],
                    "properties": {
                        "d": true
                    }
                }
            ]
        }
    },
    "oneOf": [
        {
            "$ref": "#/$defs/one"
        },
        {
            "required": [
                "a"
            ],
            "properties": {
                "a": true
            }
        }
    ],
    "unevaluatedProperties": false
}

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "dynamicevalationinsidenestedrefs"
        
def test_c__d_is_invalid(db_conn):
    data = {"c": 1, "d": 1}
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "$defs": {
        "one": {
            "oneOf": [
                {
                    "$ref": "#/$defs/two"
                },
                {
                    "required": [
                        "b"
                    ],
                    "properties": {
                        "b": true
                    }
                },
                {
                    "required": [
                        "xx"
                    ],
                    "patternProperties": {
                        "x": true
                    }
                },
                {
                    "required": [
                        "all"
                    ],
                    "unevaluatedProperties": true
                }
            ]
        },
        "two": {
            "oneOf": [
                {
                    "required": [
                        "c"
                    ],
                    "properties": {
                        "c": true
                    }
                },
                {
                    "required": [
                        "d"
                    ],
                    "properties": {
                        "d": true
                    }
                }
            ]
        }
    },
    "oneOf": [
        {
            "$ref": "#/$defs/one"
        },
        {
            "required": [
                "a"
            ],
            "properties": {
                "a": true
            }
        }
    ],
    "unevaluatedProperties": false
}

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "dynamicevalationinsidenestedrefs"
        
def test_xx_is_valid(db_conn):
    data = {"xx": 1}
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "$defs": {
        "one": {
            "oneOf": [
                {
                    "$ref": "#/$defs/two"
                },
                {
                    "required": [
                        "b"
                    ],
                    "properties": {
                        "b": true
                    }
                },
                {
                    "required": [
                        "xx"
                    ],
                    "patternProperties": {
                        "x": true
                    }
                },
                {
                    "required": [
                        "all"
                    ],
                    "unevaluatedProperties": true
                }
            ]
        },
        "two": {
            "oneOf": [
                {
                    "required": [
                        "c"
                    ],
                    "properties": {
                        "c": true
                    }
                },
                {
                    "required": [
                        "d"
                    ],
                    "properties": {
                        "d": true
                    }
                }
            ]
        }
    },
    "oneOf": [
        {
            "$ref": "#/$defs/one"
        },
        {
            "required": [
                "a"
            ],
            "properties": {
                "a": true
            }
        }
    ],
    "unevaluatedProperties": false
}

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "dynamicevalationinsidenestedrefs"
        
def test_xx__foox_is_valid(db_conn):
    data = {"xx": 1, "foox": 1}
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "$defs": {
        "one": {
            "oneOf": [
                {
                    "$ref": "#/$defs/two"
                },
                {
                    "required": [
                        "b"
                    ],
                    "properties": {
                        "b": true
                    }
                },
                {
                    "required": [
                        "xx"
                    ],
                    "patternProperties": {
                        "x": true
                    }
                },
                {
                    "required": [
                        "all"
                    ],
                    "unevaluatedProperties": true
                }
            ]
        },
        "two": {
            "oneOf": [
                {
                    "required": [
                        "c"
                    ],
                    "properties": {
                        "c": true
                    }
                },
                {
                    "required": [
                        "d"
                    ],
                    "properties": {
                        "d": true
                    }
                }
            ]
        }
    },
    "oneOf": [
        {
            "$ref": "#/$defs/one"
        },
        {
            "required": [
                "a"
            ],
            "properties": {
                "a": true
            }
        }
    ],
    "unevaluatedProperties": false
}

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "dynamicevalationinsidenestedrefs"
        
def test_xx__foo_is_invalid(db_conn):
    data = {"xx": 1, "foo": 1}
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "$defs": {
        "one": {
            "oneOf": [
                {
                    "$ref": "#/$defs/two"
                },
                {
                    "required": [
                        "b"
                    ],
                    "properties": {
                        "b": true
                    }
                },
                {
                    "required": [
                        "xx"
                    ],
                    "patternProperties": {
                        "x": true
                    }
                },
                {
                    "required": [
                        "all"
                    ],
                    "unevaluatedProperties": true
                }
            ]
        },
        "two": {
            "oneOf": [
                {
                    "required": [
                        "c"
                    ],
                    "properties": {
                        "c": true
                    }
                },
                {
                    "required": [
                        "d"
                    ],
                    "properties": {
                        "d": true
                    }
                }
            ]
        }
    },
    "oneOf": [
        {
            "$ref": "#/$defs/one"
        },
        {
            "required": [
                "a"
            ],
            "properties": {
                "a": true
            }
        }
    ],
    "unevaluatedProperties": false
}

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "dynamicevalationinsidenestedrefs"
        
def test_xx__a_is_invalid(db_conn):
    data = {"xx": 1, "a": 1}
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "$defs": {
        "one": {
            "oneOf": [
                {
                    "$ref": "#/$defs/two"
                },
                {
                    "required": [
                        "b"
                    ],
                    "properties": {
                        "b": true
                    }
                },
                {
                    "required": [
                        "xx"
                    ],
                    "patternProperties": {
                        "x": true
                    }
                },
                {
                    "required": [
                        "all"
                    ],
                    "unevaluatedProperties": true
                }
            ]
        },
        "two": {
            "oneOf": [
                {
                    "required": [
                        "c"
                    ],
                    "properties": {
                        "c": true
                    }
                },
                {
                    "required": [
                        "d"
                    ],
                    "properties": {
                        "d": true
                    }
                }
            ]
        }
    },
    "oneOf": [
        {
            "$ref": "#/$defs/one"
        },
        {
            "required": [
                "a"
            ],
            "properties": {
                "a": true
            }
        }
    ],
    "unevaluatedProperties": false
}

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "dynamicevalationinsidenestedrefs"
        
def test_xx__b_is_invalid(db_conn):
    data = {"xx": 1, "b": 1}
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "$defs": {
        "one": {
            "oneOf": [
                {
                    "$ref": "#/$defs/two"
                },
                {
                    "required": [
                        "b"
                    ],
                    "properties": {
                        "b": true
                    }
                },
                {
                    "required": [
                        "xx"
                    ],
                    "patternProperties": {
                        "x": true
                    }
                },
                {
                    "required": [
                        "all"
                    ],
                    "unevaluatedProperties": true
                }
            ]
        },
        "two": {
            "oneOf": [
                {
                    "required": [
                        "c"
                    ],
                    "properties": {
                        "c": true
                    }
                },
                {
                    "required": [
                        "d"
                    ],
                    "properties": {
                        "d": true
                    }
                }
            ]
        }
    },
    "oneOf": [
        {
            "$ref": "#/$defs/one"
        },
        {
            "required": [
                "a"
            ],
            "properties": {
                "a": true
            }
        }
    ],
    "unevaluatedProperties": false
}

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "dynamicevalationinsidenestedrefs"
        
def test_xx__c_is_invalid(db_conn):
    data = {"xx": 1, "c": 1}
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "$defs": {
        "one": {
            "oneOf": [
                {
                    "$ref": "#/$defs/two"
                },
                {
                    "required": [
                        "b"
                    ],
                    "properties": {
                        "b": true
                    }
                },
                {
                    "required": [
                        "xx"
                    ],
                    "patternProperties": {
                        "x": true
                    }
                },
                {
                    "required": [
                        "all"
                    ],
                    "unevaluatedProperties": true
                }
            ]
        },
        "two": {
            "oneOf": [
                {
                    "required": [
                        "c"
                    ],
                    "properties": {
                        "c": true
                    }
                },
                {
                    "required": [
                        "d"
                    ],
                    "properties": {
                        "d": true
                    }
                }
            ]
        }
    },
    "oneOf": [
        {
            "$ref": "#/$defs/one"
        },
        {
            "required": [
                "a"
            ],
            "properties": {
                "a": true
            }
        }
    ],
    "unevaluatedProperties": false
}

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "dynamicevalationinsidenestedrefs"
        
def test_xx__d_is_invalid(db_conn):
    data = {"xx": 1, "d": 1}
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "$defs": {
        "one": {
            "oneOf": [
                {
                    "$ref": "#/$defs/two"
                },
                {
                    "required": [
                        "b"
                    ],
                    "properties": {
                        "b": true
                    }
                },
                {
                    "required": [
                        "xx"
                    ],
                    "patternProperties": {
                        "x": true
                    }
                },
                {
                    "required": [
                        "all"
                    ],
                    "unevaluatedProperties": true
                }
            ]
        },
        "two": {
            "oneOf": [
                {
                    "required": [
                        "c"
                    ],
                    "properties": {
                        "c": true
                    }
                },
                {
                    "required": [
                        "d"
                    ],
                    "properties": {
                        "d": true
                    }
                }
            ]
        }
    },
    "oneOf": [
        {
            "$ref": "#/$defs/one"
        },
        {
            "required": [
                "a"
            ],
            "properties": {
                "a": true
            }
        }
    ],
    "unevaluatedProperties": false
}

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "dynamicevalationinsidenestedrefs"
        
def test_all_is_valid(db_conn):
    data = {"all": 1}
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "$defs": {
        "one": {
            "oneOf": [
                {
                    "$ref": "#/$defs/two"
                },
                {
                    "required": [
                        "b"
                    ],
                    "properties": {
                        "b": true
                    }
                },
                {
                    "required": [
                        "xx"
                    ],
                    "patternProperties": {
                        "x": true
                    }
                },
                {
                    "required": [
                        "all"
                    ],
                    "unevaluatedProperties": true
                }
            ]
        },
        "two": {
            "oneOf": [
                {
                    "required": [
                        "c"
                    ],
                    "properties": {
                        "c": true
                    }
                },
                {
                    "required": [
                        "d"
                    ],
                    "properties": {
                        "d": true
                    }
                }
            ]
        }
    },
    "oneOf": [
        {
            "$ref": "#/$defs/one"
        },
        {
            "required": [
                "a"
            ],
            "properties": {
                "a": true
            }
        }
    ],
    "unevaluatedProperties": false
}

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "dynamicevalationinsidenestedrefs"
        
def test_all__foo_is_valid(db_conn):
    data = {"all": 1, "foo": 1}
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "$defs": {
        "one": {
            "oneOf": [
                {
                    "$ref": "#/$defs/two"
                },
                {
                    "required": [
                        "b"
                    ],
                    "properties": {
                        "b": true
                    }
                },
                {
                    "required": [
                        "xx"
                    ],
                    "patternProperties": {
                        "x": true
                    }
                },
                {
                    "required": [
                        "all"
                    ],
                    "unevaluatedProperties": true
                }
            ]
        },
        "two": {
            "oneOf": [
                {
                    "required": [
                        "c"
                    ],
                    "properties": {
                        "c": true
                    }
                },
                {
                    "required": [
                        "d"
                    ],
                    "properties": {
                        "d": true
                    }
                }
            ]
        }
    },
    "oneOf": [
        {
            "$ref": "#/$defs/one"
        },
        {
            "required": [
                "a"
            ],
            "properties": {
                "a": true
            }
        }
    ],
    "unevaluatedProperties": false
}

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "dynamicevalationinsidenestedrefs"
        
def test_all__a_is_invalid(db_conn):
    data = {"all": 1, "a": 1}
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "$defs": {
        "one": {
            "oneOf": [
                {
                    "$ref": "#/$defs/two"
                },
                {
                    "required": [
                        "b"
                    ],
                    "properties": {
                        "b": true
                    }
                },
                {
                    "required": [
                        "xx"
                    ],
                    "patternProperties": {
                        "x": true
                    }
                },
                {
                    "required": [
                        "all"
                    ],
                    "unevaluatedProperties": true
                }
            ]
        },
        "two": {
            "oneOf": [
                {
                    "required": [
                        "c"
                    ],
                    "properties": {
                        "c": true
                    }
                },
                {
                    "required": [
                        "d"
                    ],
                    "properties": {
                        "d": true
                    }
                }
            ]
        }
    },
    "oneOf": [
        {
            "$ref": "#/$defs/one"
        },
        {
            "required": [
                "a"
            ],
            "properties": {
                "a": true
            }
        }
    ],
    "unevaluatedProperties": false
}

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "dynamicevalationinsidenestedrefs"
        
def test_ignores_booleans(db_conn):
    data = true
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "unevaluatedProperties": false
}

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "nonobjectinstancesarevalid"
        
def test_ignores_integers(db_conn):
    data = 123
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "unevaluatedProperties": false
}

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "nonobjectinstancesarevalid"
        
def test_ignores_floats(db_conn):
    data = 1.0
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "unevaluatedProperties": false
}

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "nonobjectinstancesarevalid"
        
def test_ignores_arrays(db_conn):
    data = []
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "unevaluatedProperties": false
}

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "nonobjectinstancesarevalid"
        
def test_ignores_strings(db_conn):
    data = 'foo'
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "unevaluatedProperties": false
}

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "nonobjectinstancesarevalid"
        
def test_ignores_null(db_conn):
    data = null
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "unevaluatedProperties": false
}

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "nonobjectinstancesarevalid"
        
def test_allows_null_valued_properties(db_conn):
    data = {"foo": null}
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "unevaluatedProperties": {
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

    assert result is True, "unevaluatedPropertieswithnullvaluedinstanceproperties"
        
def test_allows_only_number_properties(db_conn):
    data = {"a": 1}
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "propertyNames": {
        "maxLength": 1
    },
    "unevaluatedProperties": {
        "type": "number"
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

    assert result is True, "unevaluatedPropertiesnotaffectedbypropertyNames"
        
def test_string_property_is_invalid(db_conn):
    data = {"a": "b"}
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "propertyNames": {
        "maxLength": 1
    },
    "unevaluatedProperties": {
        "type": "number"
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

    assert result is False, "unevaluatedPropertiesnotaffectedbypropertyNames"
        
def test_valid_in_case_if_is_evaluated(db_conn):
    data = {"foo": "a"}
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "if": {
        "patternProperties": {
            "foo": {
                "type": "string"
            }
        }
    },
    "unevaluatedProperties": false
}

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is True, "unevaluatedPropertiescanseeannotationsfromifwithoutthenandelse"
        
def test_invalid_in_case_if_is_evaluated(db_conn):
    data = {"bar": "a"}
    schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "if": {
        "patternProperties": {
            "foo": {
                "type": "string"
            }
        }
    },
    "unevaluatedProperties": false
}

    data_str = json.dumps(data) if not isinstance(data, str) else data
    schema_str = json.dumps(schema)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT validate_schema(%s::jsonb, %s::jsonb) AS is_valid;",
            (data_str, schema_str)
        )
        result = cur.fetchone()[0]

    assert result is False, "unevaluatedPropertiescanseeannotationsfromifwithoutthenandelse"
        