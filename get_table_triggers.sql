--======== FUNCTION FOR TABLE TRIGGERS ===============

CREATE OR REPLACE FUNCTION get_table_triggers(p_schema_tables text[])
RETURNS TABLE (
    table_schema text,
    table_name text,
    trigger_name text,
    trigger_definition text
)
AS $$
BEGIN
    RETURN QUERY
    WITH split_input AS (
      SELECTs
        split_part(s, '.', 1) AS schema_name,
        split_part(s, '.', 2) AS table_name
      FROM unnest(p_schema_tables) AS s
    )
    SELECT
        n.nspname::text,
        c.relname::text,
        t.tgname::text,
        pg_catalog.pg_get_triggerdef(t.oid, true)::text
    FROM pg_catalog.pg_trigger AS t
    JOIN pg_catalog.pg_class AS c ON c.oid = t.tgrelid
    JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace
    JOIN split_input AS si ON si.schema_name = n.nspname AND si.table_name = c.relname
    -- A trigger's tgisinternal column will be true for system-generated triggers (e.g., for foreign keys)
    WHERE t.tgisinternal IS FALSE;
END;
$$ LANGUAGE plpgsql;

--SELECT * FROM get_table_triggers(ARRAY['public.employees', 'sales.orders']);

