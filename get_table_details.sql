--- 1. Create a function to return columns and details from a given table in table format 
CREATE OR REPLACE FUNCTION get_table_details(
    p_table_list TEXT[]  -- array of schema.table strings
)
RETURNS TABLE (
    schema_name TEXT,
    table_name TEXT,
    column_name TEXT,
    data_type TEXT,
    is_nullable TEXT,
    column_default TEXT,
    character_maximum_length INT,
    ordinal_position INT,
    md5 TEXT
)
AS $$
BEGIN
    RETURN QUERY
    SELECT
        c.table_schema::text AS schema_name,
        c.table_name::text AS table_name,
        c.column_name::text,
        c.data_type::text,
        c.is_nullable::text,
        c.column_default::text,
        c.character_maximum_length::int,
        c.ordinal_position::int,
        md5(
            c.table_schema || '.' ||
            c.table_name || '.' ||
            c.column_name || ':' ||
            c.data_type || ':' ||
            c.is_nullable || ':' ||
            COALESCE(c.column_default,'') || ':' ||
            COALESCE(c.character_maximum_length::text,'') || ':' ||
            c.ordinal_position::text
        ) AS md5
    FROM information_schema.columns c
    WHERE (c.table_schema || '.' || c.table_name) = ANY(p_table_list)
    ORDER BY c.table_schema, c.table_name, c.ordinal_position;
END;
$$ LANGUAGE plpgsql;
--SELECT * FROM get_table_details(ARRAY['public.employees', 'sales.orders']);