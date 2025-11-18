--============FUNCTION FOR TABLE INDEXES ==========================
CREATE OR REPLACE FUNCTION get_table_indexes(p_schema_tables text[])
RETURNS TABLE (
    schema_name text,
    tablename text,
    indexname text,
    tablespace text,
    indexdef text,
    md5 text
)
AS $$
BEGIN
    RETURN QUERY
    WITH split_input AS (
      SELECT
        split_part(s, '.', 1) AS schema_name,
        split_part(s, '.', 2) AS table_name
      FROM unnest(p_schema_tables) AS s
    )
    SELECT
        n.nspname::text AS schema_name,
        c.relname::text AS tablename,
        ci.relname::text AS indexname,
        COALESCE(ts.spcname, 'default')::text AS tablespace,
        pg_catalog.pg_get_indexdef(i.indexrelid)::text AS indexdef,
        md5(
            n.nspname || '.' ||
            c.relname || '.' ||
            ci.relname || '.' ||
            COALESCE(ts.spcname, 'default') || '::' ||
            pg_catalog.pg_get_indexdef(i.indexrelid)
        ) AS md5
    FROM pg_catalog.pg_class AS c
    JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace
    JOIN pg_catalog.pg_index AS i ON i.indrelid = c.oid
    JOIN pg_catalog.pg_class AS ci ON ci.oid = i.indexrelid
    LEFT JOIN pg_catalog.pg_tablespace AS ts ON ts.oid = ci.reltablespace
    JOIN split_input AS si ON si.schema_name = n.nspname AND si.table_name = c.relname
    WHERE c.relkind = 'r';
END;
$$ LANGUAGE plpgsql;
-- SELECT * FROM get_table_indexes(ARRAY['public.employees', 'sales.orders']);
