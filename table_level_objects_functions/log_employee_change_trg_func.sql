-- for trigger

CREATE OR REPLACE FUNCTION public.log_employees_change()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
BEGIN
    RAISE NOTICE 'Employee % was modified!', OLD.first_name;
    RETURN NULL;
END;
$function$

CREATE TRIGGER employees_change_trigger
AFTER UPDATE ON public.employees
FOR EACH ROW
EXECUTE FUNCTION public.log_employees_change();