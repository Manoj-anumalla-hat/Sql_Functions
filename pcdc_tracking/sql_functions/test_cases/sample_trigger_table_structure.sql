--=== Table structure ======

CREATE SCHEMA IF NOT EXISTS analytics_schema;
--==============================
CREATE TABLE analytics_schema.departments (
    department_id SERIAL PRIMARY KEY,
    department_name VARCHAR(100) NOT NULL,
    main_location VARCHAR(100),
    ternary_location VARCHAR(100),
    manager_id INT,
    budget_code VARCHAR(50)
);
--==============================
CREATE TABLE analytics_schema.employees (
    employee_id SERIAL PRIMARY KEY,
    first_name VARCHAR(100) NOT NULL,
    last_name VARCHAR(100),
    email VARCHAR(150) UNIQUE NOT NULL,
    phone_number VARCHAR(20),
    hire_date DATE NOT NULL DEFAULT CURRENT_DATE,
    salary NUMERIC(10,2),
    department_id INT NOT NULL REFERENCES analytics_schema.departments(department_id)
);
--==============================


--- Trigger Functions -----

CREATE OR REPLACE FUNCTION analytics_schema.fn_check_salary()
RETURNS trigger AS $$
BEGIN
    IF NEW.salary < 0 THEN
        RAISE EXCEPTION 'Salary cannot be negative';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
--==============================
CREATE OR REPLACE FUNCTION analytics_schema.fn_employee_insert_audit()
RETURNS trigger AS $$
BEGIN
    RAISE NOTICE 'Employee % inserted at %', NEW.employee_id, now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
--==============================
CREATE OR REPLACE FUNCTION analytics_schema.fn_department_update_audit()
RETURNS trigger AS $$
BEGIN
    RAISE NOTICE 'Department % updated at %', NEW.department_id, now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

--==============================
CREATE OR REPLACE FUNCTION analytics_schema.fn_employee_delete_cleanup()
RETURNS trigger AS $$
BEGIN
    RAISE NOTICE 'Cleanup for deleted employee %', OLD.employee_id;
    RETURN OLD;
END;
$$ LANGUAGE plpgsql;

--==============================

-- Triggers -----

CREATE TRIGGER trg_check_salary
BEFORE INSERT OR UPDATE ON analytics_schema.employees
FOR EACH ROW
EXECUTE FUNCTION analytics_schema.fn_check_salary();

--==============================

CREATE TRIGGER trg_employee_insert_audit
AFTER INSERT ON analytics_schema.employees
FOR EACH ROW
EXECUTE FUNCTION analytics_schema.fn_employee_insert_audit();
--==============================

CREATE TRIGGER trg_department_update_audit
AFTER UPDATE ON analytics_schema.departments
FOR EACH ROW
EXECUTE FUNCTION analytics_schema.fn_department_update_audit();
--==============================

CREATE TRIGGER trg_employee_delete_cleanup
AFTER DELETE ON analytics_schema.employees
FOR EACH ROW
EXECUTE FUNCTION analytics_schema.fn_employee_delete_cleanup();

--==============================

-- Statement-level Trigger Example
CREATE OR REPLACE FUNCTION analytics_schema.fn_statement_audit()
RETURNS trigger AS $$
BEGIN
    RAISE NOTICE 'Statement-level audit executed';
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_employees_stmt_audit
AFTER UPDATE ON analytics_schema.employees
FOR EACH STATEMENT
EXECUTE FUNCTION analytics_schema.fn_statement_audit();

--==============================

-- Test the trigger details function
SELECT * FROM get_trigger_details(ARRAY['analytics_schema']);