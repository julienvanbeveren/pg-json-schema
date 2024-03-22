CREATE OR REPLACE FUNCTION validate_schema(data jsonb, schema jsonb, _full_schema jsonb default NULL)
RETURNS BOOLEAN AS $$
DECLARE
  path TEXT[] DEFAULT '{}';
  _key TEXT;
  _value TEXT;
  _key2 TEXT;
  _value2 JSONB;
  _jsonb_value JSONB;
  _required TEXT[];
  _required_item TEXT;
  _boolean_value BOOLEAN;
  _number_value NUMERIC;
  _path TEXT[];
BEGIN

  IF _full_schema IS NULL THEN
    _full_schema := schema;
  END IF;

  IF schema->>'$ref' IS NOT NULL THEN
    SELECT string_to_array(regexp_replace(schema->>'$ref', '^#/', ''), '/') INTO _path;
    IF NOT validate_schema(data #> _path, schema, _full_schema) THEN
      RETURN FALSE;
    END IF;
  END IF;

  IF schema->>'enum' IS NOT NULL THEN
    _boolean_value := FALSE;
    FOR _jsonb_value IN
      SELECT jsonb_array_elements(schema->'enum')
    LOOP
      IF data = _jsonb_value THEN
        _boolean_value := TRUE;
      END IF;
    END LOOP;
    IF NOT _boolean_value THEN
      RETURN FALSE;
    END IF;
  END IF;

  IF schema->>'type' = 'array' THEN
    IF NOT jsonb_typeof(data) = 'array' THEN
      RETURN FALSE;
    END IF;
  END IF;

  IF schema->>'oneOf' IS NOT NULL THEN
    _number_value := 0;
    FOR _jsonb_value IN
      SELECT jsonb_array_elements(schema->'oneOf')
    LOOP
      IF validate_schema(data, _jsonb_value, _full_schema) THEN
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
      IF NOT validate_schema(data, _jsonb_value, _full_schema) THEN
        RETURN FALSE;
      END IF;
    END LOOP;
  END IF;

  IF schema->>'anyOf' IS NOT NULL THEN
    _boolean_value := FALSE;
    FOR _jsonb_value IN
      SELECT jsonb_array_elements(schema->'anyOf')
    LOOP
      IF validate_schema(data, _jsonb_value, _full_schema) THEN
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
    IF NOT _key = ANY(_required) AND NOT data ? _key THEN
      CONTINUE;
    END IF;
    IF NOT validate_schema(data->_key, schema->'properties'->_key, _full_schema) THEN
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

  IF jsonb_typeof(data) = 'object' THEN
    FOR _key, _value IN
      SELECT * FROM jsonb_each_text(schema->'patternProperties')
    LOOP
      FOR _key2, _value2 IN
        SELECT * FROM jsonb_each(data)
      LOOP
        IF _key2 ~ _key THEN
          IF NOT validate_schema(data->_key2, schema->'patternProperties'->_key, _full_schema) THEN
            RETURN FALSE;
          END IF;
        END IF;
      END LOOP;
    END LOOP;

    _jsonb_value := schema->'additionalProperties';
    IF _jsonb_value IS NOT NULL THEN
      <<outer>>
      FOR _key, _value IN SELECT * FROM jsonb_each_text(data)
      LOOP
        IF schema->'properties' ? _key THEN
          CONTINUE outer;
        END IF;
        <<inner>>
        FOR _key2, _value2 IN
          SELECT * FROM jsonb_each(schema->'patternProperties')
        LOOP
          IF _key ~ _key2 THEN
            CONTINUE outer;
          END IF;
        END LOOP inner;
        IF jsonb_typeof(_jsonb_value) = 'boolean' AND NOT _jsonb_value::BOOLEAN THEN
          RETURN FALSE;
        END IF;
        IF NOT validate_schema(data->_key, _jsonb_value, _full_schema) THEN
          RETURN FALSE;
        END IF;
      END LOOP outer;
    END IF;

    _jsonb_value := schema->'unevaluatedProperties';
    IF _jsonb_value IS NOT NULL THEN
      <<outer>>
      FOR _key, _value IN SELECT * FROM jsonb_each_text(data)
      LOOP
        IF schema->'properties' ? _key THEN
          CONTINUE outer;
        END IF;
        <<inner>>
        FOR _key2, _value2 IN
          SELECT * FROM jsonb_each(schema->'patternProperties')
        LOOP
          IF _key ~ _key2 THEN
            CONTINUE outer;
          END IF;
        END LOOP inner;
        IF jsonb_typeof(_jsonb_value) = 'boolean' AND NOT _jsonb_value::BOOLEAN THEN
          RETURN FALSE;
        END IF;
        IF NOT validate_schema(data->_key, _jsonb_value, _full_schema) THEN
          RETURN FALSE;
        END IF;
      END LOOP outer;
    END IF;

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

  IF schema->>'maxLength' IS NOT NULL AND jsonb_typeof(data) = 'string' THEN
    IF length((array_to_json(array[data]))->>0) > (schema->>'maxLength')::NUMERIC THEN
      RETURN FALSE;
    END IF;
  END IF;

  IF schema->>'minLength' IS NOT NULL AND jsonb_typeof(data) = 'string' THEN
    IF length((array_to_json(array[data]))->>0) < (schema->>'minLength')::NUMERIC THEN
      RETURN FALSE;
    END IF;
  END IF;

  IF schema->>'pattern' IS NOT NULL AND jsonb_typeof(data) = 'string' THEN
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

  IF schema->>'multipleOf' IS NOT NULL AND jsonb_typeof(data) = 'number' THEN
    IF (data::NUMERIC % (schema->>'multipleOf')::NUMERIC) != 0 THEN
      RETURN FALSE;
    END IF;
  END IF;

  IF schema->>'minimum' IS NOT NULL AND jsonb_typeof(data) = 'number' THEN
    IF data::NUMERIC < (schema->>'minimum')::NUMERIC THEN
      RETURN FALSE;
    END IF;
  END IF;

  IF schema->>'maximum' IS NOT NULL AND jsonb_typeof(data) = 'number' THEN
    IF data::NUMERIC > (schema->>'maximum')::NUMERIC THEN
      RETURN FALSE;
    END IF;
  END IF;

  IF schema->>'exclusiveMinimum' IS NOT NULL AND jsonb_typeof(data) = 'number' THEN
    IF data::NUMERIC <= (schema->>'exclusiveMinimum')::NUMERIC THEN
      RETURN FALSE;
    END IF;
  END IF;

  IF schema->>'exclusiveMaximum' IS NOT NULL AND jsonb_typeof(data) = 'number' THEN
    IF data::NUMERIC >= (schema->>'exclusiveMaximum')::NUMERIC THEN
      RETURN FALSE;
    END IF;
  END IF;

  IF schema->>'not' IS NOT NULL THEN
    IF validate_schema(data, schema->'not', _full_schema) THEN
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
