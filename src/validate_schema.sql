CREATE OR REPLACE FUNCTION validate_schema(data jsonb, schema jsonb)
RETURNS BOOLEAN AS $$
DECLARE
  path TEXT[] DEFAULT '{}';
BEGIN

  -- null validation
  IF schema->>'type' = 'null' THEN
    IF NOT jsonb_typeof(data) = 'null' THEN
      RETURN FALSE;
    END IF;
  END IF;

  -- boolean validation
  IF schema->>'type' = 'boolean' THEN
    IF NOT jsonb_typeof(data) = 'boolean' THEN
      RETURN FALSE;
    END IF;
  END IF;

  -- string validation
  IF schema->>'type' = 'string' THEN
    IF NOT jsonb_typeof(data) = 'string' THEN
      RETURN FALSE;
    END IF;

    IF schema->>'maxLength' IS NOT NULL THEN
      IF length((array_to_json(array[data]))->>0) > (schema->>'maxLength')::NUMERIC THEN
        RETURN FALSE;
      END IF;
    END IF;

    IF schema->>'minLength' IS NOT NULL THEN
      IF length((array_to_json(array[data]))->>0) < (schema->>'minLength')::NUMERIC THEN
        RETURN FALSE;
      END IF;
    END IF;

    IF schema->>'pattern' IS NOT NULL THEN
      IF NOT (array_to_json(array[data]))->>0 ~ (schema->>'pattern')::TEXT THEN
        RETURN FALSE;
      END IF;
    END IF;
  END IF;

  -- integer validation
  IF schema->>'type' = 'integer' THEN
    IF data::NUMERIC <> FLOOR(data::NUMERIC) THEN
      RETURN FALSE;
    END IF;
  END IF;

  -- number validation
  IF (schema->>'type' = 'number') OR (schema->>'type' = 'integer') THEN
    IF NOT jsonb_typeof(data) = 'number' THEN
      RETURN FALSE;
    END IF;

    IF schema->>'multipleOf' IS NOT NULL THEN
      IF (data::NUMERIC % (schema->>'multipleOf')::NUMERIC) != 0 THEN
        RETURN FALSE;
      END IF;
    END IF;

    IF schema->>'minimum' IS NOT NULL THEN
      IF data::NUMERIC < (schema->>'minimum')::NUMERIC THEN
        RETURN FALSE;
      END IF;
    END IF;

    IF schema->>'maximum' IS NOT NULL THEN
      IF data::NUMERIC > (schema->>'maximum')::NUMERIC THEN
        RETURN FALSE;
      END IF;
    END IF;

    IF schema->>'exclusiveMinimum' IS NOT NULL THEN
      IF data::NUMERIC <= (schema->>'exclusiveMinimum')::NUMERIC THEN
        RETURN FALSE;
      END IF;
    END IF;

    IF schema->>'exclusiveMaximum' IS NOT NULL THEN
      IF data::NUMERIC >= (schema->>'exclusiveMaximum')::NUMERIC THEN
        RETURN FALSE;
      END IF;
    END IF;
  END IF;

  RETURN TRUE;
END;
$$ LANGUAGE plpgsql;
