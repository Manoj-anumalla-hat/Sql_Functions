-- for trigger

CREATE OR REPLACE FUNCTION sales.validate_order_data()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
BEGIN
    IF NEW.total_amount < 0 THEN
        RAISE EXCEPTION 'Order total amount cannot be negative.';
    END IF;
    RETURN NEW;
END;
$function$

CREATE OR REPLACE TRIGGER before_order_insert_validate
BEFORE INSERT ON sales.orders
FOR EACH ROW
EXECUTE FUNCTION sales.validate_order_data();
