
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
    -- Base tables used inside the view definition
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
),

dependent_views AS (
    -- Views that depend on this view
    SELECT
        src_ns.nspname AS base_schema,
        src.relname AS base_view,
        string_agg(
            DISTINCT dep_ns.nspname || '.' || dep.relname,
            ', ' ORDER BY dep_ns.nspname || '.' || dep.relname
        ) AS dependent_views
    FROM pg_class src
    JOIN pg_namespace src_ns ON src.relnamespace = src_ns.oid
    JOIN pg_depend d ON d.refobjid = src.oid
    JOIN pg_rewrite r ON r.oid = d.objid
    JOIN pg_class dep ON dep.oid = r.ev_class
    JOIN pg_namespace dep_ns ON dep.relnamespace = dep_ns.oid
    WHERE src.relkind IN ('v','m')
      AND dep.relkind IN ('v','m')
      AND dep.relname <> src.relname
    GROUP BY src_ns.nspname, src.relname
),

materialized_indexes AS (
    SELECT
        v_ns.nspname AS schema_name,
        v.relname AS view_name,
        string_agg(i.relname, ', ' ORDER BY i.relname) AS indexes
    FROM pg_class v
    JOIN pg_namespace v_ns ON v.relnamespace = v_ns.oid
    JOIN pg_index ix ON ix.indrelid = v.oid
    JOIN pg_class i ON i.oid = ix.indexrelid
    WHERE v.relkind = 'm'
    GROUP BY v_ns.nspname, v.relname
),

instead_of_triggers AS (
    SELECT
        n.nspname AS schema_name,
        c.relname AS view_name,
        string_agg(t.tgname, ', ' ORDER BY t.tgname) AS triggers
    FROM pg_trigger t
    JOIN pg_class c ON c.oid = t.tgrelid
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE c.relkind = 'v'
      AND (t.tgtype & 64) <> 0
    GROUP BY n.nspname, c.relname
)

SELECT
    v_ns.nspname::TEXT AS schema_name,
    'Table' AS object_type,
    v.relname::TEXT AS object_type_name,
    ''::TEXT AS object_subtype,
    ''::TEXT AS object_subtype_name,

    concat_ws(', ',
        'view_type:' || CASE WHEN v.relkind='m' THEN 'MATERIALIZED VIEW' ELSE 'VIEW' END,
        'base_tables:' || COALESCE(vbt.base_tables,''),
        'is_materialized:' || (v.relkind='m'),
        'view_owner:' || pg_get_userbyid(v.relowner),
        'Dependent views:' || COALESCE(dv.dependent_views,'NONE'),
        'Indexes:' || COALESCE(mi.indexes,'NONE'),
        'INSTEAD OF triggers:' || COALESCE(it.triggers,'NONE'),
        'view_definition:' || pg_get_viewdef(v.oid,true)
    ) AS object_subtype_details,

    md5(
        concat_ws(':',
            CASE WHEN v.relkind='m' THEN 'MATERIALIZED VIEW' ELSE 'VIEW' END,
            pg_get_viewdef(v.oid,true),
            COALESCE(vbt.base_tables,''),
            COALESCE(dv.dependent_views,''),
            COALESCE(mi.indexes,''),
            COALESCE(it.triggers,''),
            pg_get_userbyid(v.relowner)
        )
    ) AS object_md5

FROM pg_class v
JOIN pg_namespace v_ns ON v_ns.oid = v.relnamespace
LEFT JOIN view_base_tables vbt
  ON vbt.schema_name=v_ns.nspname AND vbt.view_name=v.relname
LEFT JOIN dependent_views dv
  ON dv.base_schema=v_ns.nspname AND dv.base_view=v.relname
LEFT JOIN materialized_indexes mi
  ON mi.schema_name=v_ns.nspname AND mi.view_name=v.relname
LEFT JOIN instead_of_triggers it
  ON it.schema_name=v_ns.nspname AND it.view_name=v.relname

WHERE v.relkind IN ('v','m')
  AND v_ns.nspname NOT IN ('pg_catalog','information_schema')
  AND (
        p_schema_list IS NULL
        OR v_ns.nspname = ANY(p_schema_list)
  )
ORDER BY schema_name, object_type_name;

$function$;

-- SELECT * FROM pdcd_schema.get_view_md5();