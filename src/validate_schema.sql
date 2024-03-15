CREATE OR REPLACE FUNCTION validate_schema(data jsonb, schema jsonb)
RETURNS BOOLEAN AS $$
BEGIN

  IF schema->>'type' = 'number' THEN

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
