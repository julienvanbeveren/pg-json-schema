CREATE OR REPLACE FUNCTION validate_schema(data jsonb, schema jsonb)
RETURNS BOOLEAN AS $$
DECLARE
  path TEXT[] DEFAULT '{}';
  _key TEXT;
  _value TEXT;
  _jsonb_value JSONB;
  _required TEXT[];
  _required_item TEXT;
  _boolean_value BOOLEAN;
  _number_value NUMERIC;
BEGIN

  IF schema->>'oneOf' IS NOT NULL THEN
    _number_value := 0;
    FOR _jsonb_value IN
      SELECT jsonb_array_elements(schema->'oneOf')
    LOOP
      IF validate_schema(data, _jsonb_value) THEN
        _number_value := _number_value + 1;
      END IF;
    END LOOP;
    IF NOT _number_value = 1 THEN
      RETURN FALSE;
    END IF;
  END IF;

  IF schema->>'allOf' IS NOT NULL THEN
    FOR _jsonb_value IN
      SELECT jsonb_array_elements(schema->'allOf')
    LOOP
      IF NOT validate_schema(data, _jsonb_value) THEN
        RETURN FALSE;
      END IF;
    END LOOP;
  END IF;

  IF schema->>'anyOf' IS NOT NULL THEN
    _boolean_value := FALSE;
    FOR _jsonb_value IN
      SELECT jsonb_array_elements(schema->'anyOf')
    LOOP
      IF validate_schema(data, _jsonb_value) THEN
        _boolean_value := TRUE;
      END IF;
    END LOOP;
    IF _boolean_value = FALSE THEN
      RETURN FALSE;
    END IF;
  END IF;

  IF schema ? 'const' THEN
    IF NOT schema->'const' = data THEN
      RETURN FALSE;
    END IF;
  END IF;

  IF jsonb_typeof(schema) = 'boolean' THEN
    RETURN schema;
  END IF;

  IF schema->>'maxProperties' IS NOT NULL AND jsonb_typeof(data) = 'object' THEN
    IF (SELECT array_length(array_agg(key), 1) FROM jsonb_object_keys(data::JSONB) as key) > (schema->>'maxProperties')::NUMERIC THEN
      RETURN FALSE;
    END IF;
  END IF;

  IF schema->>'type' = 'object' THEN
    IF NOT jsonb_typeof(data) = 'object' THEN
      RETURN FALSE;
    END IF;
  END IF;

  SELECT array_agg(value) INTO _required
  FROM jsonb_array_elements_text((SELECT schema->'required')) AS value;

  FOR _key, _value IN
    SELECT * FROM jsonb_each_text(schema->'properties')
  LOOP
    IF NOT validate_schema(data->_key, schema->'properties'->_key) THEN
      RETURN FALSE;
    END IF;
  END LOOP;
  IF array_length(_required, 1) > 0 AND jsonb_typeof(data) = 'object' THEN
    FOREACH _required_item IN ARRAY _required
    LOOP
      IF NOT data ? _required_item OR THEN
        RETURN FALSE;
      END IF;
    END LOOP;
  END IF;

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

  RETURN TRUE;

  EXCEPTION
    WHEN OTHERS THEN
      RAISE NOTICE 'An error occurred: %, SQLSTATE: %', SQLERRM, SQLSTATE;
      RETURN FALSE;
END;
$$ LANGUAGE plpgsql;
