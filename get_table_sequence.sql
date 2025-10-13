--======== TABLE SEQUENCE ================

CREATE OR REPLACE FUNCTION get_table_sequences(table_names text[])
RETURNS TABLE(
  table_schema name,
  table_name name,
  sequence_name name
) AS $$
DECLARE
  fq_table text;
  schema_name text;
  name_only text;
BEGIN
  -- Loop through each schema.tablename string in the input array
  FOREACH fq_table IN ARRAY table_names
  LOOP
    -- Extract schema and table name
    schema_name := split_part(fq_table, '.', 1);
    name_only := split_part(fq_table, '.', 2);

    -- Find sequences owned by columns of the current table
    RETURN QUERY
    SELECT
        t.table_schema::name,
        t.table_name::name,
        SPLIT_PART(t.column_default, '''', 2)::name
    FROM
        information_schema.columns AS t
    WHERE
        t.table_schema = schema_name
        AND t.table_name = name_only
        AND t.column_default LIKE 'nextval%';
  END LOOP;
END;
$$ LANGUAGE plpgsql;

-- SELECT * FROM get_table_sequences(ARRAY['public.customer_orders','public.orders','public.customers','sales.orders','sales.employees','public.employees']);
