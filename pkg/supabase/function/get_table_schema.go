package function

const GetTableSchemaSQL = `
CREATE OR REPLACE FUNCTION get_table_schema(p_table_name text)
RETURNS json
LANGUAGE plpgsql
AS $$
DECLARE
    result json;
BEGIN
    SELECT json_build_object(
        'table_name', p_table_name,
        'columns', (
            SELECT json_agg(
                json_build_object(
                    'column_name', column_name,
                    'data_type', data_type,
                    'is_nullable', (is_nullable = 'YES')::boolean,
                    'column_default', COALESCE(column_default, '')
                )
            )
            FROM information_schema.columns
            WHERE table_schema = 'public'
              AND table_name = p_table_name
            ORDER BY ordinal_position
        )
    ) INTO result;

    RETURN result;
END;
$$;

GRANT EXECUTE ON FUNCTION get_table_schema(text) TO anon, authenticated;
`
