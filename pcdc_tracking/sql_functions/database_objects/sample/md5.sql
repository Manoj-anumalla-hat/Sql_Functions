CREATE OR REPLACE FUNCTION pdcd_schema.get_view_md5(
    p_schema_list TEXT[] DEFAULT NULL
)
RETURNS TABLE(
    schema_name TEXT,
    object_type TEXT,
    object_type_name TEXT,
    object_subtype TEXT,
    object_subtype_name TEXT,
    object_subtype_details TEXT,
    object_md5 TEXT
)
LANGUAGE sql
AS $function$

WITH view_base_tables AS (
    SELECT
        v_ns.nspname AS schema_name,
        v.relname AS view_name,
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
    WHERE v.relkind IN ('v','m')
      AND bt.relkind = 'r'
    GROUP BY v_ns.nspname, v.relname
)

SELECT DISTINCT
    v_ns.nspname::TEXT AS schema_name,
    'Table' AS object_type,

    /*  This should remain VIEW NAME */
    v.relname::TEXT AS object_type_name,

    /* Required empty as requested */
    ''::TEXT AS object_subtype,
    ''::TEXT AS object_subtype_name,

    /* Human readable details */
    concat_ws(',',
        'view_type:' || CASE WHEN v.relkind='m' THEN 'MATERIALIZED VIEW' ELSE 'VIEW' END,
        'base_tables:' || COALESCE(vbt.base_tables,''),
        'is_materialized:' || (v.relkind='m'),
        'view_owner:' || pg_get_userbyid(v.relowner),
        'view_definition:' || pg_get_viewdef(v.oid,true)
        'dependent_objects:' || COALESCE(vbt.base_tables,'NONE')
    ) AS object_subtype_details,

    /* Stable MD5 for rename-support */
    md5(
        concat_ws(':',
            'view_type:' || CASE WHEN v.relkind='m' THEN 'MATERIALIZED VIEW' ELSE 'VIEW' END,
            'view_definition:' || pg_get_viewdef(v.oid,true),
            'base_tables:' || COALESCE(vbt.base_tables,''),
            'is_materialized:' || (v.relkind='m'),
            'view_owner:' || pg_get_userbyid(v.relowner)
        )
    ) AS object_md5

FROM pg_class v
JOIN pg_namespace v_ns ON v_ns.oid = v.relnamespace
LEFT JOIN view_base_tables vbt
       ON vbt.schema_name = v_ns.nspname
      AND vbt.view_name = v.relname

WHERE v.relkind IN ('v','m')
  AND v_ns.nspname NOT IN ('pg_catalog','information_schema')
  AND (
        p_schema_list IS NULL
        OR v_ns.nspname = ANY(p_schema_list)
  )
ORDER BY schema_name, object_type_name;

$function$;


-- SELECT * FROM pdcd_schema.get_view_md5();


CREATE OR REPLACE FUNCTION pdcd_schema.get_view_md5_check(
    p_schema_list TEXT[] DEFAULT NULL
)
RETURNS TABLE(
    schema_name TEXT,
    object_type TEXT,
    object_type_name TEXT,
    object_subtype TEXT,
    object_subtype_name TEXT,
    object_subtype_details TEXT,
    object_md5 TEXT
)
LANGUAGE sql
AS $function$

WITH base_tables AS (
    SELECT
        v_ns.nspname AS schema_name,
        v.relname AS view_name,
        string_agg(DISTINCT bt_ns.nspname || '.' || bt.relname, ', ') AS base_tables
    FROM pg_class v
    JOIN pg_namespace v_ns ON v.relnamespace = v_ns.oid
    JOIN pg_rewrite r ON r.ev_class = v.oid
    JOIN pg_depend d ON d.objid = r.oid
    JOIN pg_class bt ON bt.oid = d.refobjid
    JOIN pg_namespace bt_ns ON bt_ns.oid = bt.relnamespace
    WHERE v.relkind IN ('v','m') AND bt.relkind = 'r'
    GROUP BY v_ns.nspname, v.relname
),

dependent_views AS (
    SELECT
        bt_ns.nspname AS schema_name,
        bt.relname AS base_view,
        string_agg(DISTINCT dv_ns.nspname || '.' || dv.relname, ', ') AS dep_views
    FROM pg_depend d
    JOIN pg_rewrite r ON d.objid = r.oid
    JOIN pg_class dv ON dv.oid = r.ev_class
    JOIN pg_class bt ON bt.oid = d.refobjid
    JOIN pg_namespace dv_ns ON dv.relnamespace = dv_ns.oid
    JOIN pg_namespace bt_ns ON bt.relnamespace = bt_ns.oid
    WHERE dv.relkind IN ('v','m') AND bt.relkind IN ('v','m')
    GROUP BY bt_ns.nspname, bt.relname
),

mv_indexes AS (
    SELECT
        n.nspname AS schema_name,
        c.relname AS view_name,
        string_agg(i.relname, ', ') AS indexes
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    JOIN pg_index x ON x.indrelid = c.oid
    JOIN pg_class i ON i.oid = x.indexrelid
    WHERE c.relkind = 'm'
    GROUP BY n.nspname, c.relname
),

instead_triggers AS (
    SELECT
        n.nspname AS schema_name,
        c.relname AS view_name,
        string_agg(t.tgname, ', ') AS triggers
    FROM pg_trigger t
    JOIN pg_class c ON c.oid = t.tgrelid
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE c.relkind = 'v' AND (t.tgtype & 64) <> 0
    GROUP BY n.nspname, c.relname
)

SELECT
    v_ns.nspname AS schema_name,
    'Table' AS object_type,
    v.relname AS object_type_name,
    '' AS object_subtype,
    '' AS object_subtype_name,

    concat_ws(', ',
        'view_type:' || CASE WHEN v.relkind='m' THEN 'MATERIALIZED VIEW' ELSE 'VIEW' END,
        'base_tables:' || COALESCE(bt.base_tables,''),
        'is_materialized:' || (v.relkind='m'),
        'view_owner:' || pg_get_userbyid(v.relowner),

        CASE WHEN dv.dep_views IS NOT NULL THEN
            'Dependent views: ' || dv.dep_views END,

        CASE WHEN v.relkind='m' AND mi.indexes IS NOT NULL THEN
            'Indexes: ' || mi.indexes END,

        CASE WHEN v.relkind='v' AND it.triggers IS NOT NULL THEN
            'INSTEAD OF triggers: ' || it.triggers END
    ) AS object_subtype_details,

    md5(
        concat_ws(':',
            'view_type:' || CASE WHEN v.relkind='m' THEN 'MATERIALIZED VIEW' ELSE 'VIEW' END,
            'view_definition:' || pg_get_viewdef(v.oid,true),
            'base_tables:' || COALESCE(bt.base_tables,''),
            'is_materialized:' || (v.relkind='m'),
            'view_owner:' || pg_get_userbyid(v.relowner)
        )
    ) AS object_md5

FROM pg_class v
JOIN pg_namespace v_ns ON v_ns.oid = v.relnamespace
LEFT JOIN base_tables bt ON bt.schema_name = v_ns.nspname AND bt.view_name = v.relname
LEFT JOIN dependent_views dv ON dv.schema_name = v_ns.nspname AND dv.base_view = v.relname
LEFT JOIN mv_indexes mi ON mi.schema_name = v_ns.nspname AND mi.view_name = v.relname
LEFT JOIN instead_triggers it ON it.schema_name = v_ns.nspname AND it.view_name = v.relname
WHERE v.relkind IN ('v','m')
  AND v_ns.nspname NOT IN ('pg_catalog','information_schema')
  AND (p_schema_list IS NULL OR v_ns.nspname = ANY(p_schema_list))
ORDER BY schema_name, object_type_name;

$function$;

-- SELECT * FROM pdcd_schema.get_view_md5_check();