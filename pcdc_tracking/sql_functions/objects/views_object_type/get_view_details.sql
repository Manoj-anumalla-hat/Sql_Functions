-- =============================================================
-- FUNCTION 1 : get_view_details (ONLY NORMAL VIEWS)
-- =============================================================
CREATE OR REPLACE FUNCTION pdcd_schema.get_view_details(
    p_schema_list TEXT[] DEFAULT NULL
)
RETURNS TABLE (
    schema_name        TEXT,
    view_name          TEXT,
    view_type          TEXT,
    view_definition    TEXT,
    base_tables        TEXT,
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
    WHERE v.relkind = 'v'
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

instead_of_triggers AS (
    SELECT
        n.nspname AS schema_name,
        c.relname AS view_name,
        string_agg(t.tgname, ', ') AS triggers
    FROM pg_trigger t
    JOIN pg_class c ON c.oid = t.tgrelid
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE c.relkind = 'v'
      AND (t.tgtype & 64) <> 0
    GROUP BY n.nspname, c.relname
)

SELECT
    v_ns.nspname::TEXT AS schema_name,
    v.relname::TEXT    AS view_name,
    'VIEW'::TEXT       AS view_type,
    pg_get_viewdef(v.oid, true)::TEXT AS view_definition,
    COALESCE(bt.base_tables, '') AS base_tables,
    pg_get_userbyid(v.relowner)::TEXT AS view_owner,

    trim(both ', ' FROM concat_ws(', ',
        CASE WHEN dv.dependent_views IS NOT NULL
             THEN 'Dependent views: ' || dv.dependent_views END,
        CASE WHEN it.triggers IS NOT NULL
             THEN 'INSTEAD OF triggers: ' || it.triggers END
    )) AS dependent_objects

FROM pg_class v
JOIN pg_namespace v_ns ON v_ns.oid = v.relnamespace
LEFT JOIN base_tables_cte bt
       ON bt.view_schema = v_ns.nspname AND bt.view_name = v.relname
LEFT JOIN dependent_views dv
       ON dv.base_schema = v_ns.nspname AND dv.base_view = v.relname
LEFT JOIN instead_of_triggers it
       ON it.schema_name = v_ns.nspname AND it.view_name = v.relname

WHERE v.relkind = 'v'
  AND v_ns.nspname NOT IN ('pg_catalog','information_schema')
  AND (
        p_schema_list IS NULL
        OR v_ns.nspname = ANY(p_schema_list)
  )
ORDER BY schema_name, view_name;

$function$;


-- select * from pdcd_schema.get_view_details(ARRAY['test_mv']);



---======================= 
--- get_view_details_new (BOTH NORMAL & MATERIALIZED VIEWS) 
--==========================

-- CREATE OR REPLACE FUNCTION pdcd_schema.get_view_details_new(
--     p_schema_list TEXT[] DEFAULT NULL
-- )
-- RETURNS TABLE (
--     schema_name        TEXT,
--     view_name          TEXT,
--     view_type          TEXT,
--     view_definition    TEXT,
--     base_tables        TEXT,
--     is_materialized    BOOLEAN,
--     is_populated       BOOLEAN,
--    -- last_refresh_time  TIMESTAMP,
--     view_owner         TEXT,
--     dependent_objects  TEXT
-- )
-- LANGUAGE sql
-- AS $function$

-- WITH base_tables_cte AS (
--     SELECT
--         v_ns.nspname AS view_schema,
--         v.relname    AS view_name,
--         string_agg(
--             DISTINCT bt_ns.nspname || '.' || bt.relname,
--             ', ' ORDER BY bt_ns.nspname || '.' || bt.relname
--         ) AS base_tables
--     FROM pg_class v
--     JOIN pg_namespace v_ns ON v.relnamespace = v_ns.oid
--     JOIN pg_rewrite r ON r.ev_class = v.oid
--     JOIN pg_depend d ON d.objid = r.oid
--     JOIN pg_class bt ON bt.oid = d.refobjid
--     JOIN pg_namespace bt_ns ON bt_ns.oid = bt.relnamespace
--     WHERE v.relkind IN ('v','m')
--       AND bt.relkind = 'r'
--     GROUP BY v_ns.nspname, v.relname
-- ),

-- /* Views built on top of this view */
-- dependent_views AS (
--     SELECT
--         base_ns.nspname AS base_schema,
--         base_v.relname  AS base_view,
--         string_agg(
--             DISTINCT child_ns.nspname || '.' || child_v.relname,
--             ', '
--         ) AS dependent_views
--     FROM pg_depend d
--     JOIN pg_rewrite r ON r.oid = d.objid
--     JOIN pg_class base_v ON base_v.oid = r.ev_class
--     JOIN pg_namespace base_ns ON base_v.relnamespace = base_ns.oid

--     JOIN pg_class child_v ON child_v.oid = d.refobjid
--     JOIN pg_namespace child_ns ON child_v.relnamespace = child_ns.oid

--     WHERE base_v.relkind IN ('v','m')
--       AND child_v.relkind IN ('v','m')
--       AND base_v.oid <> child_v.oid
--     GROUP BY base_ns.nspname, base_v.relname
-- ),

-- /* Indexes only for materialized views */
-- index_deps AS (
--     SELECT
--         n.nspname AS schema_name,
--         c.relname AS view_name,
--         string_agg(i.relname, ', ') AS indexes
--     FROM pg_class c
--     JOIN pg_namespace n ON c.relnamespace = n.oid
--     JOIN pg_index x ON x.indrelid = c.oid
--     JOIN pg_class i ON i.oid = x.indexrelid
--     WHERE c.relkind = 'm'
--     GROUP BY n.nspname, c.relname
-- ),

-- /* INSTEAD OF triggers for normal views */
-- instead_of_triggers AS (
--     SELECT
--         n.nspname AS schema_name,
--         c.relname AS view_name,
--         string_agg(t.tgname, ', ') AS triggers
--     FROM pg_trigger t
--     JOIN pg_class c ON c.oid = t.tgrelid
--     JOIN pg_namespace n ON n.oid = c.relnamespace
--     WHERE c.relkind = 'v'
--       AND (t.tgtype & 64) <> 0
--     GROUP BY n.nspname, c.relname
-- ),

-- /* Best effort refresh time tracking */
-- matview_stats AS (
--     SELECT
--         schemaname,
--         relname
--        -- last_analyze AS last_refresh_time
--     FROM pg_stat_user_tables
-- )

-- SELECT
--     v_ns.nspname::TEXT AS schema_name,
--     v.relname::TEXT    AS view_name,

--     CASE WHEN v.relkind = 'm' THEN 'MATERIALIZED VIEW' ELSE 'VIEW' END AS view_type,
--     pg_get_viewdef(v.oid, true)::TEXT AS view_definition,
--     COALESCE(bt.base_tables, '') AS base_tables,

--     (v.relkind = 'm') AS is_materialized,

--     CASE 
--         WHEN v.relkind = 'm' THEN NOT v.relispopulated IS FALSE 
--         ELSE NULL
--     END AS is_populated,

--     -- CASE
--     --     WHEN v.relkind = 'm' THEN ms.last_refresh_time
--     --     ELSE NULL
--     -- END AS last_refresh_time,

--     pg_get_userbyid(v.relowner)::TEXT AS view_owner,

--     trim(both ', ' FROM concat_ws(', ',
--         CASE WHEN dv.dependent_views IS NOT NULL 
--              THEN 'Dependent views: ' || dv.dependent_views END,
--         CASE WHEN v.relkind = 'm' AND id.indexes IS NOT NULL
--              THEN 'Indexes: ' || id.indexes END,
--         CASE WHEN v.relkind = 'v' AND it.triggers IS NOT NULL
--              THEN 'INSTEAD OF triggers: ' || it.triggers END
--     )) AS dependent_objects

-- FROM pg_class v
-- JOIN pg_namespace v_ns ON v_ns.oid = v.relnamespace
-- LEFT JOIN base_tables_cte bt
--        ON bt.view_schema = v_ns.nspname AND bt.view_name = v.relname
-- LEFT JOIN dependent_views dv
--        ON dv.base_schema = v_ns.nspname AND dv.base_view = v.relname
-- LEFT JOIN index_deps id
--        ON id.schema_name = v_ns.nspname AND id.view_name = v.relname
-- LEFT JOIN instead_of_triggers it
--        ON it.schema_name = v_ns.nspname AND it.view_name = v.relname
-- LEFT JOIN matview_stats ms
--        ON ms.schemaname = v_ns.nspname AND ms.relname = v.relname

-- WHERE v.relkind IN ('v','m')
--   AND v_ns.nspname NOT IN ('pg_catalog','information_schema')
--   AND (
--         p_schema_list IS NULL 
--         OR v_ns.nspname = ANY(p_schema_list)
--   )
-- ORDER BY schema_name, view_name;

-- $function$;

-- select * from pdcd_schema.get_view_details_new(ARRAY['hr']);

-- -- \i '/Users/manoj_anumalla/Documents/GitHub/Sql_Functions/pcdc_tracking/sql_functions/objects_sql_functions/object_type_views/get_view_details.sql'


