--====== TABLE OWNER AND PERMISSIONS ===========

CREATE OR REPLACE FUNCTION get_table_owners_permissions(table_names text[])
RETURNS TABLE (
    schemaname name,
    tablename name,
    object_name name,
    object_type text,
    ownername name,
    privileges text,
    md5 text
)
LANGUAGE sql
AS $$
WITH target_tables AS (
    -- Collect the schema and name of the specified tables
    SELECT
        n.nspname AS schemaname,
        c.relname AS tablename,
        c.oid AS table_oid
    FROM
        pg_class c
    JOIN
        pg_namespace n ON n.oid = c.relnamespace
    JOIN
        unnest(table_names) AS t(fq_name) ON t.fq_name = n.nspname || '.' || c.relname
    WHERE
        c.relkind = 'r'
),
all_related_oids AS (
    -- Collect the OIDs of the target tables and all dependent objects
    SELECT table_oid AS oid FROM target_tables
    UNION
    SELECT objid FROM pg_depend WHERE refobjid IN (SELECT table_oid FROM target_tables)
    UNION
    SELECT refobjid FROM pg_depend WHERE objid IN (SELECT table_oid FROM target_tables)
)
-- Main query to get ownership and privileges for all collected objects
SELECT
    tt.schemaname,
    tt.tablename,
    c.relname AS object_name,
    CASE c.relkind
        WHEN 'r' THEN 'Table'
        WHEN 'i' THEN 'Index'
        WHEN 'S' THEN 'Sequence'
        WHEN 'v' THEN 'View'
        WHEN 'f' THEN 'Foreign Table'
        WHEN 'p' THEN 'Partitioned Table'
        WHEN 't' THEN 'Toast Table'
        ELSE c.relkind::text
    END AS object_type,
    pg_get_userbyid(c.relowner) AS ownername,
    COALESCE(
        pg_catalog.array_to_string(c.relacl, E'\n'),
        'Default (All for owner)'
    ) AS privileges,
    md5(
        tt.schemaname || '.' ||
        tt.tablename || '.' ||
        c.relname || '.' ||
        CASE c.relkind
            WHEN 'r' THEN 'Table'
            WHEN 'i' THEN 'Index'
            WHEN 'S' THEN 'Sequence'
            WHEN 'v' THEN 'View'
            WHEN 'f' THEN 'Foreign Table'
            WHEN 'p' THEN 'Partitioned Table'
            WHEN 't' THEN 'Toast Table'
            ELSE c.relkind::text
        END || '.' ||
        pg_get_userbyid(c.relowner)
    ) AS md5
FROM
    pg_class c
JOIN
    pg_namespace n ON n.oid = c.relnamespace
JOIN
    all_related_oids aro ON c.oid = aro.oid
JOIN
    target_tables tt ON tt.table_oid = (
        -- Find the OID of the base table this object depends on
        SELECT CASE
            WHEN c.relkind IN ('r', 'v', 'f') THEN c.oid
            ELSE (SELECT refobjid FROM pg_depend WHERE objid = c.oid AND deptype = 'a' LIMIT 1)
        END
    )
WHERE
    n.nspname NOT IN ('pg_catalog', 'information_schema')
    OR c.relkind IN ('r', 'S', 'i');
$$;

-- SELECT * FROM get_table_owners_permissions(ARRAY['public.customer_orders','public.orders','public.customers','sales.orders','sales.employees','public.employees']);