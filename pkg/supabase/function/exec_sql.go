package function

const ExecSQL = `
CREATE OR REPLACE FUNCTION exec_sql(query text)
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
AS $$
BEGIN
  EXECUTE query;
END;
$$;

GRANT EXECUTE ON FUNCTION exec_sql(text) TO service_role;
`
