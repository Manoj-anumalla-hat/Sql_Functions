--===== TABLE VIEW DEFINATION with schemaname.view =========

CREATE OR REPLACE FUNCTION get_view_definitions(p_schema_views text[])
RETURNS TABLE (
    view_schema text,
    view_name text,
    view_definition text
)
AS $$
BEGIN
    RETURN QUERY
    WITH split_input AS (
      SELECT
        split_part(s, '.', 1) AS schema_name,
        split_part(s, '.', 2) AS view_name
      FROM unnest(p_schema_views) AS s
    )
    SELECT
        n.nspname::text AS view_schema,
        c.relname::text AS view_name,
        pg_catalog.pg_get_viewdef(c.oid, true)::text AS view_definition
    FROM pg_catalog.pg_class AS c
    JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace
    JOIN split_input AS si ON si.schema_name = n.nspname AND si.view_name = c.relname
    WHERE c.relkind = 'v';
END;
$$ LANGUAGE plpgsql;

--==== TABLE VIEW ======
CREATE VIEW public.customer_orders AS
SELECT o.order_id, c.first_name, o.order_date
FROM orders o
JOIN customers c ON o.customer_id = c.customer_id;

--SELECT * FROM get_view_definitions(ARRAY['public.customer_orders']);