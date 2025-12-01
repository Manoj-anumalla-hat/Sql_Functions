CREATE OR REPLACE FUNCTION pdcd_schema.get_mat_views_md5(
    p_table_list TEXT[] DEFAULT NULL
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
LANGUAGE plpgsql
AS $function$
BEGIN
    RETURN QUERY
    SELECT
        gtd.schema_name,
        'Materialized View' AS object_type,
        gtd.view_name AS object_type_name,
        NULL::TEXT AS object_subtype,
        NULL::TEXT AS object_subtype_name,

        concat_ws(
            ',',
            'view_type:' || COALESCE(gtd.view_type, ''),
            'view_definition:' || COALESCE(gtd.view_definition, ''),
            'base_tables:' || COALESCE(gtd.base_tables, ''),
           -- 'is_materialized:' || COALESCE(gtd.is_materialized::TEXT, ''),
            'is_ populated:' || COALESCE(gtd.is_populated::TEXT, ''),
            -- 'last_refresh_time:' || COALESCE(gtd.last_refresh_time::TEXT,
            'view_owner:' || COALESCE(gtd.view_owner, ''),
            'dependent_objects:' || COALESCE(gtd.dependent_objects, '')
        ) AS object_subtype_details,

        md5(
            concat_ws(
                ':',
                'view_type:' || COALESCE(gtd.view_type, ''),
                'view_definition:' || COALESCE(gtd.view_definition, ''),
                'base_tables:' || COALESCE(gtd.base_tables, ''),
                -- 'is_materialized:' || COALESCE(gtd.is_materialized::TEXT, ''),
                'is_ populated:' || COALESCE(gtd.is_populated::TEXT, ''),
                -- 'last_refresh_time:' || COALESCE(gtd.last_refresh_time::TEXT,
                'view_owner:' || COALESCE(gtd.view_owner, ''),
                'dependent_objects:' || COALESCE(gtd.dependent_objects, '')
            )
        ) AS object_md5

    FROM pdcd_schema.get_mat_view_details(p_table_list) gtd
    ORDER BY gtd.schema_name, gtd.view_name;
END;
$function$;

-- SELECT * FROM pdcd_schema.get_mat_views_md5();