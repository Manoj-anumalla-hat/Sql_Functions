-- =============================================================
-- FUNCTION 2 : get_mat_view_details (ONLY MATERIALIZED VIEWS)
-- =============================================================
CREATE OR REPLACE FUNCTION pdcd_schema.get_mat_view_details(
    p_schema_list TEXT[] DEFAULT NULL
)
RETURNS TABLE (
    schema_name        TEXT,
    view_name          TEXT,
    view_type          TEXT,
    view_definition    TEXT,
    base_tables        TEXT,
    is_populated       BOOLEAN,
    view_owner         TEXT,
    dependent_objects  TEXT
)
LANGUAGE sql
AS $function$

WITH base_tables_cte AS (
    SELECT
        v_ns.nspname AS view_schema,
        v.relname    AS view_name,
        string_agg(
            DISTINCT bt_ns.nspname || '.' || bt.relname,
            ', ' ORDER BY bt_ns.nspname || '.' || bt.relname
        ) AS base_tables
    FROM pg_class v
    JOIN pg_namespace v_ns ON v.relnamespace = v_ns.oid
    JOIN pg_rewrite r ON r.ev_class = v.oid
    JOIN pg_depend d ON d.objid = r.oid
    JOIN pg_class bt ON bt.oid = d.refobjid
    JOIN pg_namespace bt_ns ON bt_ns.oid = bt.relnamespace
    WHERE v.relkind = 'm'
      AND bt.relkind = 'r'
    GROUP BY v_ns.nspname, v.relname
),

dependent_views AS (
    SELECT
        base_ns.nspname AS base_schema,
        base_v.relname  AS base_view,
        string_agg(
            DISTINCT child_ns.nspname || '.' || child_v.relname,
            ', '
        ) AS dependent_views
    FROM pg_depend d
    JOIN pg_rewrite r ON r.oid = d.objid
    JOIN pg_class base_v ON base_v.oid = r.ev_class
    JOIN pg_namespace base_ns ON base_v.relnamespace = base_ns.oid

    JOIN pg_class child_v ON child_v.oid = d.refobjid
    JOIN pg_namespace child_ns ON child_v.relnamespace = child_ns.oid

    WHERE base_v.relkind IN ('v','m')
      AND child_v.relkind IN ('v','m')
      AND base_v.oid <> child_v.oid
    GROUP BY base_ns.nspname, base_v.relname
),

index_deps AS (
    SELECT
        n.nspname AS schema_name,
        c.relname AS view_name,
        string_agg(i.relname, ', ') AS indexes
    FROM pg_class c
    JOIN pg_namespace n ON c.relnamespace = n.oid
    JOIN pg_index x ON x.indrelid = c.oid
    JOIN pg_class i ON i.oid = x.indexrelid
    WHERE c.relkind = 'm'
    GROUP BY n.nspname, c.relname
)

SELECT
    v_ns.nspname::TEXT AS schema_name,
    v.relname::TEXT    AS view_name,
    'MATERIALIZED VIEW'::TEXT AS view_type,
    -- Explicit for materialized views,
    pg_get_viewdef(v.oid, true)::TEXT AS view_definition,
    COALESCE(bt.base_tables, '') AS base_tables,
    v.relispopulated AS is_populated,
    pg_get_userbyid(v.relowner)::TEXT AS view_owner,

    trim(both ', ' FROM concat_ws(', ',
        CASE WHEN dv.dependent_views IS NOT NULL
             THEN 'Dependent views: ' || dv.dependent_views END,
        CASE WHEN id.indexes IS NOT NULL
             THEN 'Indexes: ' || id.indexes END
    )) AS dependent_objects

FROM pg_class v
JOIN pg_namespace v_ns ON v_ns.oid = v.relnamespace
LEFT JOIN base_tables_cte bt
       ON bt.view_schema = v_ns.nspname AND bt.view_name = v.relname
LEFT JOIN dependent_views dv
       ON dv.base_schema = v_ns.nspname AND dv.base_view = v.relname
LEFT JOIN index_deps id
       ON id.schema_name = v_ns.nspname AND id.view_name = v.relname

WHERE v.relkind = 'm'
  AND v_ns.nspname NOT IN ('pg_catalog','information_schema')
  AND (
        p_schema_list IS NULL
        OR v_ns.nspname = ANY(p_schema_list)
  )
ORDER BY schema_name, view_name;

$function$;


-- select * from pdcd_schema.get_mat_view_details(ARRAY['pdcd_test']);