CREATE OR REPLACE FUNCTION validate_schema(data jsonb, schema jsonb)
RETURNS BOOLEAN AS $$
BEGIN
  IF schema->>'type' = 'number' THEN
    IF NOT jsonb_typeof(data) = 'number' THEN
      RETURN FALSE;
    END IF;
  END IF;

  RETURN TRUE;

END;
$$ LANGUAGE plpgsql;
