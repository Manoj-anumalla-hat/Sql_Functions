--========= TABLE CONSTRAINTS FUNCTION ==========

CREATE OR REPLACE FUNCTION get_table_constraints(p_schema_tables text[])
RETURNS TABLE (
    constraint_name text,
    constraint_type text,
    column_name text,
    definition text,
    table_name text,
    md5 text
)
AS $$
DECLARE
    schema_table text;
    schema_name text;
    table_name text;
BEGIN
    -- Loop through each schema.table entry in the input array
    FOREACH schema_table IN ARRAY p_schema_tables
    LOOP
        -- Extract schema and table names
        schema_name := split_part(schema_table, '.', 1);
        table_name := split_part(schema_table, '.', 2);
        
        -- Return query results for the current table
        RETURN QUERY
        WITH split_input AS (
          SELECT
            schema_name AS schema_name,
            table_name AS table_name
        )
        SELECT
          tc.constraint_name::text AS constraint_name,
          tc.constraint_type::text AS constraint_type,
          kcu.column_name::text AS column_name,
          pg_get_constraintdef(c.oid)::text AS definition,
          schema_table::text AS table_name,
          md5(
            COALESCE(tc.constraint_name, 'N/A') || ':' ||
            COALESCE(tc.constraint_type, 'N/A') || ':' ||
            COALESCE(kcu.column_name, 'N/A') || ':' ||
            COALESCE(pg_get_constraintdef(c.oid), 'N/A') || ':' ||
            schema_table
          ) AS md5
        FROM
          information_schema.table_constraints tc
        LEFT JOIN
          information_schema.key_column_usage kcu
          ON tc.constraint_name = kcu.constraint_name
          AND tc.table_schema = kcu.table_schema
        LEFT JOIN
          pg_catalog.pg_constraint c
          ON c.conname = tc.constraint_name
          AND c.connamespace = tc.table_schema::regnamespace
        JOIN
          split_input si
          ON tc.table_schema = si.schema_name
          AND tc.table_name = si.table_name

        UNION ALL

        SELECT
          NULL AS constraint_name,
          'NOT NULL' AS constraint_type,
          col.column_name::text AS column_name,
          'NOT NULL' AS definition,
          schema_table::text AS table_name,
          md5(
            'NOT NULL:' || col.column_name || ':' || schema_table
          ) AS md5
        FROM
          information_schema.columns col
        JOIN
          split_input si
          ON col.table_schema = si.schema_name
          AND col.table_name = si.table_name
        WHERE
          col.is_nullable = 'NO';
    END LOOP;
END;
$$ LANGUAGE plpgsql;

-- SELECT * FROM get_table_constraints(ARRAY['public.employees', 'sales.orders']);
