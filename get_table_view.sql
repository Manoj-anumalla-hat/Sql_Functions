--====== TABLE VIEW FUNCTION ==========

CREATE OR REPLACE FUNCTION get_table_views(p_schema_tables text[])
RETURNS TABLE (
    table_schema text,
    table_name text,
    dependent_view_schema text,
    dependent_view_name text,
    view_definition text
)
AS $$
BEGIN
    RETURN QUERY
    -- Process the input array of schema.table strings
    WITH input_tables AS (
      SELECT
        split_part(s, '.', 1) AS schema_name,
        split_part(s, '.', 2) AS table_name
      FROM unnest(p_schema_tables) AS s
    )
    SELECT DISTINCT
        st_ns.nspname::text AS table_schema,
        st_c.relname::text AS table_name,
        dv_ns.nspname::text AS dependent_view_schema,
        dv_c.relname::text AS dependent_view_name,
        pg_catalog.pg_get_viewdef(dv_c.oid, true)::text AS view_definition
    FROM pg_catalog.pg_class AS st_c -- source table class
    JOIN pg_catalog.pg_namespace AS st_ns ON st_ns.oid = st_c.relnamespace
    JOIN input_tables AS it ON it.schema_name = st_ns.nspname AND it.table_name = st_c.relname
    JOIN pg_catalog.pg_depend AS pd ON pd.refobjid = st_c.oid AND pd.deptype = 'n'
    JOIN pg_catalog.pg_rewrite AS pr ON pr.oid = pd.objid
    JOIN pg_catalog.pg_class AS dv_c ON dv_c.oid = pr.ev_class AND dv_c.relkind = 'v' -- dependent view class
    JOIN pg_catalog.pg_namespace AS dv_ns ON dv_ns.oid = dv_c.relnamespace;
END;
$$ LANGUAGE plpgsql;


CREATE VIEW public.customer_orders AS
SELECT o.order_id, c.first_name, o.order_date
FROM orders o
JOIN customers c ON o.customer_id = c.customer_id;

-- Call the function to get the view definition

--SELECT * FROM get_table_views(ARRAY['sales.orders', 'public.orders','public.customers']);
