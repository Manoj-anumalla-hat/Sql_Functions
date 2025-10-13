--======== FUNCTION FOR TABLE TRIGGERS ===============

CREATE OR REPLACE FUNCTION get_table_triggers(p_schema_tables text[])
RETURNS TABLE (
    table_schema text,
    table_name text,
    trigger_name text,
    trigger_definition text,
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
        n.nspname::text AS table_schema,
        c.relname::text AS table_name,
        t.tgname::text AS trigger_name,
        pg_catalog.pg_get_triggerdef(t.oid, true)::text AS trigger_definition,
        md5(
            n.nspname || '.' ||
            c.relname || '.' ||
            t.tgname || '::' ||
            pg_catalog.pg_get_triggerdef(t.oid, true)
        ) AS md5
    FROM pg_catalog.pg_trigger AS t
    JOIN pg_catalog.pg_class AS c ON c.oid = t.tgrelid
    JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace
    JOIN split_input AS si ON si.schema_name = n.nspname AND si.table_name = c.relname
    WHERE t.tgisinternal IS FALSE;
END;
$$ LANGUAGE plpgsql;

--SELECT * FROM get_table_triggers(ARRAY['public.employees', 'sales.orders']);

