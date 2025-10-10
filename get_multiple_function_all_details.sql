--- 2. Create a function to return the function definition and access params in the table format 
CREATE OR REPLACE FUNCTION public.get_multiple_function_all_details(func_names text[])
 RETURNS TABLE(function_name text, arg_name text, function_definition text, owner_name text, define_privileges text, execute_privileges text, security_definer boolean, is_strict boolean, volatility text, return_type text)
 LANGUAGE sql
AS $function$
WITH FUNC AS (
    SELECT
        p.proname AS function_name,
        p.oid,
        p.proargnames,
        p.prosrc,
        p.proowner,
        p.prosecdef AS security_definer,
        p.proisstrict AS is_strict,
        p.provolatile AS volatility,
        t.typname AS return_type,
        p.proacl
    FROM pg_proc p
    LEFT JOIN pg_type t ON t.oid = p.prorettype
    WHERE p.proname = ANY(func_names)
),
args AS (
    SELECT
        f.function_name,
        COALESCE(pn.n, NULL) AS arg_name,
        f.prosrc AS function_definition,
        f.proowner AS owner_oid,
        f.security_definer,
        f.is_strict,
        f.volatility,
        f.return_type,
        f.proacl
    FROM FUNC f
    LEFT JOIN LATERAL unnest(f.proargnames) WITH ORDINALITY AS pn(n, ord) ON TRUE
)
SELECT
    function_name,
    arg_name,
    function_definition,
    (SELECT rolname FROM pg_roles WHERE oid = owner_oid) AS owner_name,
    (SELECT rolname FROM pg_roles WHERE oid = owner_oid) AS define_privileges,
    CASE
        WHEN proacl IS NOT NULL THEN
            array_to_string(
                ARRAY(
                    SELECT DISTINCT
                        CASE split_part(aclitem::text, '=', 1)
                            WHEN '' THEN 'PUBLIC'
                            ELSE split_part(aclitem::text, '=', 1)
                        END
                    FROM unnest(proacl) AS aclitem
                    WHERE position('X' IN split_part(aclitem::text, '=', 2)) > 0
                    UNION
                    SELECT (SELECT rolname FROM pg_roles WHERE oid = owner_oid)
                ), ', '
            )
        ELSE 'PUBLIC, ' || (SELECT rolname FROM pg_roles WHERE oid = owner_oid)
    END AS execute_privileges,
    security_definer,
    is_strict,
    volatility,
    return_type
FROM args
ORDER BY function_name, arg_name NULLS FIRST;
$function$

-- select * from get_multiple_function_all_details(ARRAY['get_table_details']);
