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
  END IF;

  RETURN TRUE;

END;
$$ LANGUAGE plpgsql;
