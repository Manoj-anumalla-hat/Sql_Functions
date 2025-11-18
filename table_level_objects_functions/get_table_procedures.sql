--====== TABLE PROCEDURES =====

CREATE OR REPLACE FUNCTION get_procedures_for_tables(
  target_tables text[]
)
RETURNS TABLE (
  table_schema name,
  table_name name,
  proc_schema name,
  proc_name name,
  arguments text,
  definition text,
  md5 text
) AS $$
DECLARE
  fq_table text;
  sch_name name;
  tbl_name name;
BEGIN
  -- Loop through each schema.tablename string in the input array
  FOREACH fq_table IN ARRAY target_tables
  LOOP
    -- Extract schema and table name
    sch_name := split_part(fq_table, '.', 1);
    tbl_name := split_part(fq_table, '.', 2);
    
    -- Return procedures that contain the table name in their source code
    RETURN QUERY
    SELECT
      sch_name,
      tbl_name,
      n.nspname::name AS proc_schema,
      p.proname::name AS proc_name,
      pg_get_function_arguments(p.oid)::text AS arguments,
      pg_get_functiondef(p.oid)::text AS definition,
      md5(pg_get_functiondef(p.oid)::text) AS md5
    FROM
      pg_proc p
    JOIN
      pg_namespace n ON n.oid = p.pronamespace
    WHERE
      p.prokind = 'p'
      AND p.prosrc ILIKE '%' || tbl_name || '%'
      AND n.nspname = sch_name;
  END LOOP;
END;
$$ LANGUAGE plpgsql;

-- SELECT * FROM get_procedures_for_tables(ARRAY['sales.orders','public.products','public.employees']);