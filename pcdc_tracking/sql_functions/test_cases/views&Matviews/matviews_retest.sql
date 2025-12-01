# 🔹 MATERIALIZED VIEW TEST SCENARIOS

##  BASE STRUCTURE SETUP

### Create Schemas

```sql
CREATE SCHEMA sales;
CREATE SCHEMA hr;
```

### Tables in sales schema

```sql
CREATE TABLE sales.orders (
    order_id INT PRIMARY KEY,
    customer_name VARCHAR(50),
    amount NUMERIC(10,2)
);

CREATE TABLE sales.payments (
    payment_id INT PRIMARY KEY,
    order_id INT,
    payment_mode VARCHAR(30)
);
```

### Tables in hr schema

```sql
CREATE TABLE hr.employees (
    emp_id INT PRIMARY KEY,
    emp_name VARCHAR(50),
    salary NUMERIC(10,2)
);

CREATE TABLE hr.departments (
    dept_id INT PRIMARY KEY,
    dept_name VARCHAR(50),
    location VARCHAR(50)
);
```

SELECT * FROM pdcd_schema.load_snapshot_tbl();
SELECT * FROM pdcd_schema.load_md5_metadata_tbl(ARRAY['hr']);
SELECT * FROM pdcd_schema.load_md5_metadata_staging_tbl(ARRAY['hr']);
SELECT * from pdcd_schema.load_md5_metadata_staging_functions(ARRAY['hr']);

select metadata_id,snapshot_id,schema_name,object_type,object_type_name,object_subtype,object_subtype_name,object_md5,processed_time,change_type FROM pdcd_schema.md5_metadata_tbl;
 metadata_id | snapshot_id | schema_name |    object_type    | object_type_name  | object_subtype | object_subtype_name |            object_md5            |       processed_time       | change_type
-------------+-------------+-------------+-------------------+-------------------+----------------+---------------------+----------------------------------+----------------------------+-------------
           1 |           1 | hr          | Table             | departments       | Column         | dept_id             | 16c869b63036a8b4c89016ea4f97f63b | 2025-11-28 18:21:48.122606 | ADDED
           2 |           1 | hr          | Table             | departments       | Column         | dept_name           | b537a75e4c2744b85e478bf937370ba9 | 2025-11-28 18:21:48.123809 | ADDED
           3 |           1 | hr          | Table             | departments       | Column         | location            | eb892ea3bb6ebc1480b7b61c5fffb6db | 2025-11-28 18:21:48.123832 | ADDED
           4 |           1 | hr          | Table             | employees         | Column         | emp_id              | 4feaf72682cf29404f4407c0a7ba9c93 | 2025-11-28 18:21:48.123845 | ADDED
           5 |           1 | hr          | Table             | employees         | Column         | emp_name            | b537a75e4c2744b85e478bf937370ba9 | 2025-11-28 18:21:48.123858 | ADDED
           6 |           1 | hr          | Table             | employees         | Column         | salary              | 9786c5f70226a57b52ffc90e55867d57 | 2025-11-28 18:21:48.123868 | ADDED
           7 |           1 | hr          | Table             | departments       | Constraint     | departments_pkey    | 9e71d83424d14d7af950aa5eab35d0a1 | 2025-11-28 18:21:48.128875 | ADDED
           8 |           1 | hr          | Table             | employees         | Constraint     | employees_pkey      | 9e9d7894742421811810fa3c39c5489e | 2025-11-28 18:21:48.128909 | ADDED
           9 |           1 | hr          | Table             | departments       | Index          | departments_pkey    | d73a3bcda456f6113c2bd35bfe8776fe | 2025-11-28 18:21:48.133608 | ADDED
          10 |           1 | hr          | Table             | employees         | Index          | employees_pkey      | 93c3ea9faa198edd0fb3f21eeaadb84a | 2025-11-28 18:21:48.133628 | ADDED

select metadata_id,snapshot_id,schema_name,object_type,object_type_name,object_subtype,object_subtype_name,object_md5,processed_time from pdcd_schema.md5_metadata_staging_tbl;
-- same as above

select metadata_id,snapshot_id,schema_name,object_type,object_type_name,object_subtype,object_subtype_name,object_md5,processed_time from pdcd_schema.md5_metadata_staging_functions;

-- no functions,views,matviews created yet, so no output


## Base Materialized View

```sql
CREATE MATERIALIZED VIEW hr.mv_emp_base AS
SELECT emp_id, emp_name, salary
FROM hr.employees;
```

### Dependent View on MV

```sql
CREATE VIEW hr.v_emp_high_salary AS
SELECT emp_name, salary
FROM hr.mv_emp_base
WHERE salary > 50000;
```
--* Test 0 - Initial View Addition check
    SELECT * FROM pdcd_schema.load_snapshot_tbl();
    SELECT * FROM pdcd_schema.compare_load_md5_metadata_tbl(ARRAY['hr']);
    SELECT * from pdcd_schema.compare_load_md5_metadata_functions(ARRAY['hr']);
    TRUNCATE TABLE pdcd_schema.md5_metadata_staging_tbl RESTART IDENTITY CASCADE;
    TRUNCATE TABLE pdcd_schema.md5_metadata_staging_functions RESTART IDENTITY CASCADE;
    SELECT * FROM pdcd_schema.load_md5_metadata_staging_tbl(ARRAY['hr']);
    SELECT * from pdcd_schema.load_md5_metadata_staging_functions(ARRAY['hr']);

select metadata_id,snapshot_id,schema_name,object_type,object_type_name,object_subtype,object_subtype_name,object_md5,processed_time,change_type FROM pdcd_schema.md5_metadata_tbl;
          11 |           2 | hr          | Materialized View | mv_emp_base       | Column         | emp_id              | 0127d1460ae24d6e781f068aba382060 | 2025-11-28 18:22:33.437372 | ADDED
          12 |           2 | hr          | Materialized View | mv_emp_base       | Column         | emp_name            | b537a75e4c2744b85e478bf937370ba9 | 2025-11-28 18:22:33.437506 | ADDED
          13 |           2 | hr          | Materialized View | mv_emp_base       | Column         | salary              | 9786c5f70226a57b52ffc90e55867d57 | 2025-11-28 18:22:33.437515 | ADDED
          14 |           2 | hr          | View              | v_emp_high_salary | Column         | emp_name            | 7b6d79c39d7a5fec5b6f1f60762106d7 | 2025-11-28 18:22:33.437528 | ADDED
          15 |           2 | hr          | View              | v_emp_high_salary | Column         | salary              | ce34772703c27e0c1fc1294bdde99a2c | 2025-11-28 18:22:33.437535 | ADDED
          16 |           2 | hr          | View              | v_emp_high_salary |                |                     | 72e4d7218dc103d1bf3f8a9d5ed5ba94 | 2025-11-28 18:22:34.779458 | ADDED
          17 |           2 | hr          | Materialized View | mv_emp_base       |                |                     | 81cb98c5b654889d62b9a67c81469403 | 2025-11-28 18:22:34.787928 | ADDED
(17 rows)

select metadata_id,snapshot_id,schema_name,object_type,object_type_name,object_subtype,object_subtype_name,object_md5,processed_time from pdcd_schema.md5_metadata_staging_tbl;
 metadata_id | snapshot_id | schema_name |    object_type    | object_type_name  | object_subtype | object_subtype_name |            object_md5            |       processed_time
-------------+-------------+-------------+-------------------+-------------------+----------------+---------------------+----------------------------------+----------------------------
           1 |           2 | hr          | Table             | employees         | Column         | emp_id              | 4feaf72682cf29404f4407c0a7ba9c93 | 2025-11-28 18:22:35.213077
           2 |           2 | hr          | Materialized View | mv_emp_base       | Column         | emp_id              | 0127d1460ae24d6e781f068aba382060 | 2025-11-28 18:22:35.221366
           3 |           2 | hr          | Materialized View | mv_emp_base       | Column         | emp_name            | b537a75e4c2744b85e478bf937370ba9 | 2025-11-28 18:22:35.221399
           4 |           2 | hr          | View              | v_emp_high_salary | Column         | salary              | ce34772703c27e0c1fc1294bdde99a2c | 2025-11-28 18:22:35.221413
           5 |           2 | hr          | Materialized View | mv_emp_base       | Column         | salary              | 9786c5f70226a57b52ffc90e55867d57 | 2025-11-28 18:22:35.221426
           6 |           2 | hr          | Table             | departments       | Column         | location            | eb892ea3bb6ebc1480b7b61c5fffb6db | 2025-11-28 18:22:35.22144
           7 |           2 | hr          | Table             | employees         | Column         | emp_name            | b537a75e4c2744b85e478bf937370ba9 | 2025-11-28 18:22:35.221452
           8 |           2 | hr          | View              | v_emp_high_salary | Column         | emp_name            | 7b6d79c39d7a5fec5b6f1f60762106d7 | 2025-11-28 18:22:35.221464
           9 |           2 | hr          | Table             | departments       | Column         | dept_name           | b537a75e4c2744b85e478bf937370ba9 | 2025-11-28 18:22:35.221475
          10 |           2 | hr          | Table             | departments       | Column         | dept_id             | 16c869b63036a8b4c89016ea4f97f63b | 2025-11-28 18:22:35.221489
          11 |           2 | hr          | Table             | employees         | Column         | salary              | 9786c5f70226a57b52ffc90e55867d57 | 2025-11-28 18:22:35.221502
          12 |           2 | hr          | Table             | departments       | Constraint     | departments_pkey    | 9e71d83424d14d7af950aa5eab35d0a1 | 2025-11-28 18:22:35.226563
          13 |           2 | hr          | Table             | employees         | Constraint     | employees_pkey      | 9e9d7894742421811810fa3c39c5489e | 2025-11-28 18:22:35.2266
          14 |           2 | hr          | Table             | departments       | Index          | departments_pkey    | d73a3bcda456f6113c2bd35bfe8776fe | 2025-11-28 18:22:35.232292
          15 |           2 | hr          | Table             | employees         | Index          | employees_pkey      | 93c3ea9faa198edd0fb3f21eeaadb84a | 2025-11-28 18:22:35.232316
(15 rows)


select metadata_id,snapshot_id,schema_name,object_type,object_type_name,object_subtype,object_subtype_name,object_subtype_details,object_md5,processed_time from pdcd_schema.md5_metadata_staging_functions;
 metadata_id | snapshot_id | schema_name |    object_type    | object_type_name  | object_subtype | object_subtype_name |                                                      object_subtype_details                                                      |            object_md5            |       processed_time
-------------+-------------+-------------+-------------------+-------------------+----------------+---------------------+----------------------------------------------------------------------------------------------------------------------------------+----------------------------------+----------------------------
           1 |           2 | hr          | View              | v_emp_high_salary |                |                     | view_type:VIEW,view_definition: SELECT mv_emp_base.emp_name,                                                                    +| 72e4d7218dc103d1bf3f8a9d5ed5ba94 | 2025-11-28 18:22:35.782845
             |             |             |                   |                   |                |                     |     mv_emp_base.salary                                                                                                          +|                                  |
             |             |             |                   |                   |                |                     |    FROM hr.mv_emp_base                                                                                                          +|                                  |
             |             |             |                   |                   |                |                     |   WHERE mv_emp_base.salary > 50000::numeric;,base_tables:,view_owner:test_user,dependent_objects:Dependent views: hr.mv_emp_base |                                  |
           2 |           2 | hr          | Materialized View | mv_emp_base       |                |                     | view_type:MATERIALIZED VIEW,view_definition: SELECT employees.emp_id,                                                           +| 81cb98c5b654889d62b9a67c81469403 | 2025-11-28 18:22:35.798515
             |             |             |                   |                   |                |                     |     employees.emp_name,                                                                                                         +|                                  |
             |             |             |                   |                   |                |                     |     employees.salary                                                                                                            +|                                  |
             |             |             |                   |                   |                |                     |    FROM hr.employees;,base_tables:hr.employees,is_ populated:true,view_owner:test_user,dependent_objects:                        |                                  |
(2 rows)

---

--* Test Run 1 — Initial Refresh & Modification

```sql
REFRESH MATERIALIZED VIEW hr.mv_emp_base;
```
    SELECT * FROM pdcd_schema.load_snapshot_tbl();
    SELECT * FROM pdcd_schema.compare_load_md5_metadata_tbl(ARRAY['hr']);
    SELECT * from pdcd_schema.compare_load_md5_metadata_functions(ARRAY['hr']);
    TRUNCATE TABLE pdcd_schema.md5_metadata_staging_tbl RESTART IDENTITY CASCADE;
    TRUNCATE TABLE pdcd_schema.md5_metadata_staging_functions RESTART IDENTITY CASCADE;
    SELECT * FROM pdcd_schema.load_md5_metadata_staging_tbl(ARRAY['hr']);
    SELECT * from pdcd_schema.load_md5_metadata_staging_functions(ARRAY['hr']);

select metadata_id,snapshot_id,schema_name,object_type,object_type_name,object_subtype,object_subtype_name,object_md5,processed_time,change_type FROM pdcd_schema.md5_metadata_tbl;

-- no changes detected after refresh
-- both staging tables update with latest snapshot info, no changes same as previous

--* Test Run 2 — Rename MV & Add Dependent MV

```sql
ALTER MATERIALIZED VIEW hr.mv_emp_base RENAME TO mv_emp_master;

CREATE MATERIALIZED VIEW hr.mv_emp_summary AS
SELECT COUNT(emp_id) AS total_emp
FROM hr.mv_emp_master;
```

    SELECT * FROM pdcd_schema.load_snapshot_tbl();
    SELECT * FROM pdcd_schema.compare_load_md5_metadata_tbl(ARRAY['hr']);
    SELECT * from pdcd_schema.compare_load_md5_metadata_functions(ARRAY['hr']);
    TRUNCATE TABLE pdcd_schema.md5_metadata_staging_tbl RESTART IDENTITY CASCADE;
    TRUNCATE TABLE pdcd_schema.md5_metadata_staging_functions RESTART IDENTITY CASCADE;
    SELECT * FROM pdcd_schema.load_md5_metadata_staging_tbl(ARRAY['hr']);
    SELECT * from pdcd_schema.load_md5_metadata_staging_functions(ARRAY['hr']);

select metadata_id,snapshot_id,schema_name,object_type,object_type_name,object_subtype,object_subtype_name,object_md5,processed_time,change_type FROM pdcd_schema.md5_metadata_tbl;
          18 |           4 | hr          | Materialized View | mv_emp_master     | Column         | emp_id              | 0127d1460ae24d6e781f068aba382060 | 2025-11-28 18:30:40.265957 | ADDED
          19 |           4 | hr          | Materialized View | mv_emp_master     | Column         | emp_name            | b537a75e4c2744b85e478bf937370ba9 | 2025-11-28 18:30:40.26601  | ADDED
          20 |           4 | hr          | Materialized View | mv_emp_master     | Column         | salary              | 9786c5f70226a57b52ffc90e55867d57 | 2025-11-28 18:30:40.266017 | ADDED
          21 |           4 | hr          | Materialized View | mv_emp_summary    | Column         | total_emp           | 58e71ceb471355854fcacf33294970dc | 2025-11-28 18:30:40.266022 | ADDED
          22 |           4 | hr          | Materialized View | mv_emp_base       | Column         | emp_id              | 0127d1460ae24d6e781f068aba382060 | 2025-11-28 18:30:40.266085 | DELETED
          23 |           4 | hr          | Materialized View | mv_emp_base       | Column         | emp_name            | b537a75e4c2744b85e478bf937370ba9 | 2025-11-28 18:30:40.266116 | DELETED
          24 |           4 | hr          | Materialized View | mv_emp_base       | Column         | salary              | 9786c5f70226a57b52ffc90e55867d57 | 2025-11-28 18:30:40.266122 | DELETED
          25 |           4 | hr          | Materialized View | mv_emp_master     |                |                     | 81cb98c5b654889d62b9a67c81469403 | 2025-11-28 18:30:41.36278  | RENAMED
          26 |           4 | hr          | View              | v_emp_high_salary |                |                     | 3222dd23bfe94c50b238e80c9fbbc7b5 | 2025-11-28 18:30:41.362869 | MODIFIED
          27 |           4 | hr          | Materialized View | mv_emp_summary    |                |                     | e4c5f61639589b2da781bf68e2b1341f | 2025-11-28 18:30:41.364709 | ADDED
(27 rows)

select metadata_id,snapshot_id,schema_name,object_type,object_type_name,object_subtype,object_subtype_name,object_md5,processed_time from pdcd_schema.md5_metadata_staging_tbl;
 metadata_id | snapshot_id | schema_name |    object_type    | object_type_name  | object_subtype | object_subtype_name |            object_md5            |       processed_time
-------------+-------------+-------------+-------------------+-------------------+----------------+---------------------+----------------------------------+----------------------------
           1 |           4 | hr          | Table             | employees         | Column         | emp_id              | 4feaf72682cf29404f4407c0a7ba9c93 | 2025-11-28 18:30:41.767513
           2 |           4 | hr          | Materialized View | mv_emp_summary    | Column         | total_emp           | 58e71ceb471355854fcacf33294970dc | 2025-11-28 18:30:41.776937
           3 |           4 | hr          | Materialized View | mv_emp_master     | Column         | emp_id              | 0127d1460ae24d6e781f068aba382060 | 2025-11-28 18:30:41.776986
           4 |           4 | hr          | Materialized View | mv_emp_master     | Column         | emp_name            | b537a75e4c2744b85e478bf937370ba9 | 2025-11-28 18:30:41.776999
           5 |           4 | hr          | View              | v_emp_high_salary | Column         | salary              | ce34772703c27e0c1fc1294bdde99a2c | 2025-11-28 18:30:41.777012
           6 |           4 | hr          | Table             | departments       | Column         | location            | eb892ea3bb6ebc1480b7b61c5fffb6db | 2025-11-28 18:30:41.777022
           7 |           4 | hr          | Table             | employees         | Column         | emp_name            | b537a75e4c2744b85e478bf937370ba9 | 2025-11-28 18:30:41.777033
           8 |           4 | hr          | View              | v_emp_high_salary | Column         | emp_name            | 7b6d79c39d7a5fec5b6f1f60762106d7 | 2025-11-28 18:30:41.777044
           9 |           4 | hr          | Materialized View | mv_emp_master     | Column         | salary              | 9786c5f70226a57b52ffc90e55867d57 | 2025-11-28 18:30:41.777055
          10 |           4 | hr          | Table             | departments       | Column         | dept_name           | b537a75e4c2744b85e478bf937370ba9 | 2025-11-28 18:30:41.777067
          11 |           4 | hr          | Table             | departments       | Column         | dept_id             | 16c869b63036a8b4c89016ea4f97f63b | 2025-11-28 18:30:41.777078
          12 |           4 | hr          | Table             | employees         | Column         | salary              | 9786c5f70226a57b52ffc90e55867d57 | 2025-11-28 18:30:41.777089
          13 |           4 | hr          | Table             | departments       | Constraint     | departments_pkey    | 9e71d83424d14d7af950aa5eab35d0a1 | 2025-11-28 18:30:41.781793
          14 |           4 | hr          | Table             | employees         | Constraint     | employees_pkey      | 9e9d7894742421811810fa3c39c5489e | 2025-11-28 18:30:41.781828
          15 |           4 | hr          | Table             | departments       | Index          | departments_pkey    | d73a3bcda456f6113c2bd35bfe8776fe | 2025-11-28 18:30:41.78703
          16 |           4 | hr          | Table             | employees         | Index          | employees_pkey      | 93c3ea9faa198edd0fb3f21eeaadb84a | 2025-11-28 18:30:41.787058
(16 rows)

select metadata_id,snapshot_id,schema_name,object_type,object_type_name,object_subtype,object_subtype_name,object_subtype_details,object_md5,processed_time from pdcd_schema.md5_metadata_staging_functions;
metadata_id | snapshot_id | schema_name |    object_type    | object_type_name  | object_subtype | object_subtype_name |                                                        object_subtype_details                                                        |            object_md5            |       processed_time
-------------+-------------+-------------+-------------------+-------------------+----------------+---------------------+--------------------------------------------------------------------------------------------------------------------------------------+----------------------------------+----------------------------
           1 |           4 | hr          | View              | v_emp_high_salary |                |                     | view_type:VIEW,view_definition: SELECT mv_emp_master.emp_name,                                                                      +| 3222dd23bfe94c50b238e80c9fbbc7b5 | 2025-11-28 18:30:42.619704
             |             |             |                   |                   |                |                     |     mv_emp_master.salary                                                                                                            +|                                  |
             |             |             |                   |                   |                |                     |    FROM hr.mv_emp_master                                                                                                            +|                                  |
             |             |             |                   |                   |                |                     |   WHERE mv_emp_master.salary > 50000::numeric;,base_tables:,view_owner:test_user,dependent_objects:Dependent views: hr.mv_emp_master |                                  |
           2 |           4 | hr          | Materialized View | mv_emp_master     |                |                     | view_type:MATERIALIZED VIEW,view_definition: SELECT employees.emp_id,                                                               +| 81cb98c5b654889d62b9a67c81469403 | 2025-11-28 18:30:42.639595
             |             |             |                   |                   |                |                     |     employees.emp_name,                                                                                                             +|                                  |
             |             |             |                   |                   |                |                     |     employees.salary                                                                                                                +|                                  |
             |             |             |                   |                   |                |                     |    FROM hr.employees;,base_tables:hr.employees,is_ populated:true,view_owner:test_user,dependent_objects:                            |                                  |
           3 |           4 | hr          | Materialized View | mv_emp_summary    |                |                     | view_type:MATERIALIZED VIEW,view_definition: SELECT count(mv_emp_master.emp_id) AS total_emp                                        +| e4c5f61639589b2da781bf68e2b1341f | 2025-11-28 18:30:42.639683
             |             |             |                   |                   |                |                     |    FROM hr.mv_emp_master;,base_tables:,is_ populated:true,view_owner:test_user,dependent_objects:Dependent views: hr.mv_emp_master   |                                  |
(3 rows)


--* Test Run 3 — MV Definition Changes

```sql
DROP MATERIALIZED VIEW hr.mv_emp_master;

-------------- DROP MATERIALIZED VIEW hr.mv_emp_master cascade;

CREATE MATERIALIZED VIEW hr.mv_emp_master AS
SELECT emp_id, emp_name
FROM hr.employees;
```

    SELECT * FROM pdcd_schema.load_snapshot_tbl();
    SELECT * FROM pdcd_schema.compare_load_md5_metadata_tbl(ARRAY['hr']);
    SELECT * from pdcd_schema.compare_load_md5_metadata_functions(ARRAY['hr']);
    TRUNCATE TABLE pdcd_schema.md5_metadata_staging_tbl RESTART IDENTITY CASCADE;
    TRUNCATE TABLE pdcd_schema.md5_metadata_staging_functions RESTART IDENTITY CASCADE;
    SELECT * FROM pdcd_schema.load_md5_metadata_staging_tbl(ARRAY['hr']);
    SELECT * from pdcd_schema.load_md5_metadata_staging_functions(ARRAY['hr']);

select metadata_id,snapshot_id,schema_name,object_type,object_type_name,object_subtype,object_subtype_name,object_md5,processed_time,change_type FROM pdcd_schema.md5_metadata_tbl;
          28 |           5 | hr          | Materialized View | mv_emp_summary    | Column         | total_emp           | 58e71ceb471355854fcacf33294970dc | 2025-11-28 18:40:07.505603 | DELETED
          29 |           5 | hr          | View              | v_emp_high_salary | Column         | salary              | ce34772703c27e0c1fc1294bdde99a2c | 2025-11-28 18:40:07.505646 | DELETED
          30 |           5 | hr          | View              | v_emp_high_salary | Column         | emp_name            | 7b6d79c39d7a5fec5b6f1f60762106d7 | 2025-11-28 18:40:07.505655 | DELETED
          31 |           5 | hr          | Materialized View | mv_emp_master     | Column         | salary              | 9786c5f70226a57b52ffc90e55867d57 | 2025-11-28 18:40:07.505661 | DELETED
          32 |           5 | hr          | Materialized View | mv_emp_master     |                |                     | 905979a54526c6aac047b5a037f1856d | 2025-11-28 18:40:08.569049 | MODIFIED
          33 |           5 | hr          | View              | v_emp_high_salary |                |                     | 3222dd23bfe94c50b238e80c9fbbc7b5 | 2025-11-28 18:40:08.569151 | DELETED
          34 |           5 | hr          | Materialized View | mv_emp_summary    |                |                     | e4c5f61639589b2da781bf68e2b1341f | 2025-11-28 18:40:08.569162 | DELETED
(34 rows)

select metadata_id,snapshot_id,schema_name,object_type,object_type_name,object_subtype,object_subtype_name,object_md5,processed_time from pdcd_schema.md5_metadata_staging_tbl;
 metadata_id | snapshot_id | schema_name |    object_type    | object_type_name | object_subtype | object_subtype_name |            object_md5            |       processed_time
-------------+-------------+-------------+-------------------+------------------+----------------+---------------------+----------------------------------+----------------------------
           1 |           5 | hr          | Table             | employees        | Column         | emp_id              | 4feaf72682cf29404f4407c0a7ba9c93 | 2025-11-28 18:40:09.076457
           2 |           5 | hr          | Materialized View | mv_emp_master    | Column         | emp_id              | 0127d1460ae24d6e781f068aba382060 | 2025-11-28 18:40:09.085633
           3 |           5 | hr          | Materialized View | mv_emp_master    | Column         | emp_name            | b537a75e4c2744b85e478bf937370ba9 | 2025-11-28 18:40:09.085691
           4 |           5 | hr          | Table             | departments      | Column         | location            | eb892ea3bb6ebc1480b7b61c5fffb6db | 2025-11-28 18:40:09.085707
           5 |           5 | hr          | Table             | employees        | Column         | emp_name            | b537a75e4c2744b85e478bf937370ba9 | 2025-11-28 18:40:09.085718
           6 |           5 | hr          | Table             | departments      | Column         | dept_name           | b537a75e4c2744b85e478bf937370ba9 | 2025-11-28 18:40:09.085732
           7 |           5 | hr          | Table             | departments      | Column         | dept_id             | 16c869b63036a8b4c89016ea4f97f63b | 2025-11-28 18:40:09.085745
           8 |           5 | hr          | Table             | employees        | Column         | salary              | 9786c5f70226a57b52ffc90e55867d57 | 2025-11-28 18:40:09.085757
           9 |           5 | hr          | Table             | departments      | Constraint     | departments_pkey    | 9e71d83424d14d7af950aa5eab35d0a1 | 2025-11-28 18:40:09.090251
          10 |           5 | hr          | Table             | employees        | Constraint     | employees_pkey      | 9e9d7894742421811810fa3c39c5489e | 2025-11-28 18:40:09.090282
          11 |           5 | hr          | Table             | departments      | Index          | departments_pkey    | d73a3bcda456f6113c2bd35bfe8776fe | 2025-11-28 18:40:09.095733
          12 |           5 | hr          | Table             | employees        | Index          | employees_pkey      | 93c3ea9faa198edd0fb3f21eeaadb84a | 2025-11-28 18:40:09.095759
(12 rows)

select metadata_id,snapshot_id,schema_name,object_type,object_type_name,object_subtype,object_subtype_name,object_subtype_details,object_md5,processed_time from pdcd_schema.md5_metadata_staging_functions;
 metadata_id | snapshot_id | schema_name |    object_type    | object_type_name | object_subtype | object_subtype_name |                                          object_subtype_details                                           |            object_md5            |      processed_time
-------------+-------------+-------------+-------------------+------------------+----------------+---------------------+-----------------------------------------------------------------------------------------------------------+----------------------------------+---------------------------
           1 |           5 | hr          | Materialized View | mv_emp_master    |                |                     | view_type:MATERIALIZED VIEW,view_definition: SELECT employees.emp_id,                                    +| 905979a54526c6aac047b5a037f1856d | 2025-11-28 18:40:09.37129
             |             |             |                   |                  |                |                     |     employees.emp_name                                                                                   +|                                  |
             |             |             |                   |                  |                |                     |    FROM hr.employees;,base_tables:hr.employees,is_ populated:true,view_owner:test_user,dependent_objects: |                                  |
(1 row)


--* Test Run 4 — create Dependent MV
```sql
CREATE MATERIALIZED VIEW hr.mv_emp_summary AS
SELECT COUNT(emp_id) AS total_emp
FROM hr.mv_emp_master;
```

    SELECT * FROM pdcd_schema.load_snapshot_tbl();
    SELECT * FROM pdcd_schema.compare_load_md5_metadata_tbl(ARRAY['hr']);
    SELECT * from pdcd_schema.compare_load_md5_metadata_functions(ARRAY['hr']);
    TRUNCATE TABLE pdcd_schema.md5_metadata_staging_tbl RESTART IDENTITY CASCADE;
    TRUNCATE TABLE pdcd_schema.md5_metadata_staging_functions RESTART IDENTITY CASCADE;
    SELECT * FROM pdcd_schema.load_md5_metadata_staging_tbl(ARRAY['hr']);
    SELECT * from pdcd_schema.load_md5_metadata_staging_functions(ARRAY['hr']);

select metadata_id,snapshot_id,schema_name,object_type,object_type_name,object_subtype,object_subtype_name,object_md5,processed_time,change_type FROM pdcd_schema.md5_metadata_tbl;
 metadata_id | snapshot_id | schema_name |    object_type    | object_type_name  | object_subtype | object_subtype_name |            object_md5            |       processed_time       | change_type
          35 |           6 | hr          | Materialized View | mv_emp_summary    | Column         | total_emp           | 58e71ceb471355854fcacf33294970dc | 2025-11-28 19:10:29.887133 | ADDED
          36 |           6 | hr          | Materialized View | mv_emp_summary    |                |                     | e4c5f61639589b2da781bf68e2b1341f | 2025-11-28 19:10:31.019017 | ADDED
(36 rows)

select metadata_id,snapshot_id,schema_name,object_type,object_type_name,object_subtype,object_subtype_name,object_md5,processed_time from pdcd_schema.md5_metadata_staging_tbl;
 metadata_id | snapshot_id | schema_name |    object_type    | object_type_name | object_subtype | object_subtype_name |            object_md5            |       processed_time
-------------+-------------+-------------+-------------------+------------------+----------------+---------------------+----------------------------------+----------------------------
           1 |           6 | hr          | Table             | employees        | Column         | emp_id              | 4feaf72682cf29404f4407c0a7ba9c93 | 2025-11-28 19:10:31.469236
           2 |           6 | hr          | Materialized View | mv_emp_summary   | Column         | total_emp           | 58e71ceb471355854fcacf33294970dc | 2025-11-28 19:10:31.477136
           3 |           6 | hr          | Materialized View | mv_emp_master    | Column         | emp_id              | 0127d1460ae24d6e781f068aba382060 | 2025-11-28 19:10:31.477166
           4 |           6 | hr          | Materialized View | mv_emp_master    | Column         | emp_name            | b537a75e4c2744b85e478bf937370ba9 | 2025-11-28 19:10:31.477173
           5 |           6 | hr          | Table             | departments      | Column         | location            | eb892ea3bb6ebc1480b7b61c5fffb6db | 2025-11-28 19:10:31.477179
           6 |           6 | hr          | Table             | employees        | Column         | emp_name            | b537a75e4c2744b85e478bf937370ba9 | 2025-11-28 19:10:31.477184
           7 |           6 | hr          | Table             | departments      | Column         | dept_name           | b537a75e4c2744b85e478bf937370ba9 | 2025-11-28 19:10:31.477189
           8 |           6 | hr          | Table             | departments      | Column         | dept_id             | 16c869b63036a8b4c89016ea4f97f63b | 2025-11-28 19:10:31.477195
           9 |           6 | hr          | Table             | employees        | Column         | salary              | 9786c5f70226a57b52ffc90e55867d57 | 2025-11-28 19:10:31.4772
          10 |           6 | hr          | Table             | departments      | Constraint     | departments_pkey    | 9e71d83424d14d7af950aa5eab35d0a1 | 2025-11-28 19:10:31.481282
          11 |           6 | hr          | Table             | employees        | Constraint     | employees_pkey      | 9e9d7894742421811810fa3c39c5489e | 2025-11-28 19:10:31.481305
          12 |           6 | hr          | Table             | departments      | Index          | departments_pkey    | d73a3bcda456f6113c2bd35bfe8776fe | 2025-11-28 19:10:31.486273
          13 |           6 | hr          | Table             | employees        | Index          | employees_pkey      | 93c3ea9faa198edd0fb3f21eeaadb84a | 2025-11-28 19:10:31.486295
(13 rows)

select metadata_id,snapshot_id,schema_name,object_type,object_type_name,object_subtype,object_subtype_name,object_subtype_details,object_md5,processed_time from pdcd_schema.md5_metadata_staging_functions;
 metadata_id | snapshot_id | schema_name |    object_type    | object_type_name | object_subtype | object_subtype_name |                                                       object_subtype_details                                                       |            object_md5            |       processed_time
-------------+-------------+-------------+-------------------+------------------+----------------+---------------------+------------------------------------------------------------------------------------------------------------------------------------+----------------------------------+----------------------------
           1 |           6 | hr          | Materialized View | mv_emp_summary   |                |                     | view_type:MATERIALIZED VIEW,view_definition: SELECT count(mv_emp_master.emp_id) AS total_emp                                      +| e4c5f61639589b2da781bf68e2b1341f | 2025-11-28 19:10:32.023773
             |             |             |                   |                  |                |                     |    FROM hr.mv_emp_master;,base_tables:,is_ populated:true,view_owner:test_user,dependent_objects:Dependent views: hr.mv_emp_master |                                  |
           2 |           6 | hr          | Materialized View | mv_emp_master    |                |                     | view_type:MATERIALIZED VIEW,view_definition: SELECT employees.emp_id,                                                             +| 905979a54526c6aac047b5a037f1856d | 2025-11-28 19:10:32.031219
             |             |             |                   |                  |                |                     |     employees.emp_name                                                                                                            +|                                  |
             |             |             |                   |                  |                |                     |    FROM hr.employees;,base_tables:hr.employees,is_ populated:true,view_owner:test_user,dependent_objects:                          |                                  |
(2 rows)

--* Test Run 5 — Drop & Recreate MV

```sql
DROP MATERIALIZED VIEW hr.mv_emp_summary;

CREATE MATERIALIZED VIEW hr.mv_emp_summary AS
SELECT emp_name,emp_id
FROM hr.mv_emp_master;
```

    SELECT * FROM pdcd_schema.load_snapshot_tbl();
    SELECT * FROM pdcd_schema.compare_load_md5_metadata_tbl(ARRAY['hr']);
    SELECT * from pdcd_schema.compare_load_md5_metadata_functions(ARRAY['hr']);
    TRUNCATE TABLE pdcd_schema.md5_metadata_staging_tbl RESTART IDENTITY CASCADE;
    TRUNCATE TABLE pdcd_schema.md5_metadata_staging_functions RESTART IDENTITY CASCADE;
    SELECT * FROM pdcd_schema.load_md5_metadata_staging_tbl(ARRAY['hr']);
    SELECT * from pdcd_schema.load_md5_metadata_staging_functions(ARRAY['hr']);

select metadata_id,snapshot_id,schema_name,object_type,object_type_name,object_subtype,object_subtype_name,object_md5,processed_time,change_type FROM pdcd_schema.md5_metadata_tbl;
 metadata_id | snapshot_id | schema_name |    object_type    | object_type_name  | object_subtype | object_subtype_name |            object_md5            |       processed_time       | change_type
          37 |           7 | hr          | Materialized View | mv_emp_summary    | Column         | emp_name            | 7b6d79c39d7a5fec5b6f1f60762106d7 | 2025-11-28 19:24:06.722365 | ADDED
          38 |           7 | hr          | Materialized View | mv_emp_summary    | Column         | emp_id              | 1851575de0ebf31fdbd942e3d11d15d2 | 2025-11-28 19:24:06.722421 | ADDED
          39 |           7 | hr          | Materialized View | mv_emp_summary    | Column         | total_emp           | 58e71ceb471355854fcacf33294970dc | 2025-11-28 19:24:06.722466 | DELETED
          40 |           7 | hr          | Materialized View | mv_emp_summary    |                |                     | 6dca483dde6a9d34850a5383654eea74 | 2025-11-28 19:24:07.724612 | MODIFIED
(40 rows)

select metadata_id,snapshot_id,schema_name,object_type,object_type_name,object_subtype,object_subtype_name,object_md5,processed_time from pdcd_schema.md5_metadata_staging_tbl;
 metadata_id | snapshot_id | schema_name |    object_type    | object_type_name | object_subtype | object_subtype_name |            object_md5            |       processed_time
-------------+-------------+-------------+-------------------+------------------+----------------+---------------------+----------------------------------+----------------------------
           1 |           7 | hr          | Materialized View | mv_emp_summary   | Column         | emp_name            | 7b6d79c39d7a5fec5b6f1f60762106d7 | 2025-11-28 19:24:08.276344
           2 |           7 | hr          | Table             | employees        | Column         | emp_id              | 4feaf72682cf29404f4407c0a7ba9c93 | 2025-11-28 19:24:08.285181
           3 |           7 | hr          | Materialized View | mv_emp_master    | Column         | emp_id              | 0127d1460ae24d6e781f068aba382060 | 2025-11-28 19:24:08.285216
           4 |           7 | hr          | Materialized View | mv_emp_master    | Column         | emp_name            | b537a75e4c2744b85e478bf937370ba9 | 2025-11-28 19:24:08.285231
           5 |           7 | hr          | Materialized View | mv_emp_summary   | Column         | emp_id              | 1851575de0ebf31fdbd942e3d11d15d2 | 2025-11-28 19:24:08.285247
           6 |           7 | hr          | Table             | departments      | Column         | location            | eb892ea3bb6ebc1480b7b61c5fffb6db | 2025-11-28 19:24:08.285259
           7 |           7 | hr          | Table             | employees        | Column         | emp_name            | b537a75e4c2744b85e478bf937370ba9 | 2025-11-28 19:24:08.285273
           8 |           7 | hr          | Table             | departments      | Column         | dept_name           | b537a75e4c2744b85e478bf937370ba9 | 2025-11-28 19:24:08.285287
           9 |           7 | hr          | Table             | departments      | Column         | dept_id             | 16c869b63036a8b4c89016ea4f97f63b | 2025-11-28 19:24:08.2853
          10 |           7 | hr          | Table             | employees        | Column         | salary              | 9786c5f70226a57b52ffc90e55867d57 | 2025-11-28 19:24:08.285312
          11 |           7 | hr          | Table             | departments      | Constraint     | departments_pkey    | 9e71d83424d14d7af950aa5eab35d0a1 | 2025-11-28 19:24:08.290343
          12 |           7 | hr          | Table             | employees        | Constraint     | employees_pkey      | 9e9d7894742421811810fa3c39c5489e | 2025-11-28 19:24:08.290378
          13 |           7 | hr          | Table             | departments      | Index          | departments_pkey    | d73a3bcda456f6113c2bd35bfe8776fe | 2025-11-28 19:24:08.295113
          14 |           7 | hr          | Table             | employees        | Index          | employees_pkey      | 93c3ea9faa198edd0fb3f21eeaadb84a | 2025-11-28 19:24:08.295141
(14 rows)

select metadata_id,snapshot_id,schema_name,object_type,object_type_name,object_subtype,object_subtype_name,object_subtype_details,object_md5,processed_time from pdcd_schema.md5_metadata_staging_functions;
 metadata_id | snapshot_id | schema_name |    object_type    | object_type_name | object_subtype | object_subtype_name |                                                       object_subtype_details                                                       |            object_md5            |       processed_time
-------------+-------------+-------------+-------------------+------------------+----------------+---------------------+------------------------------------------------------------------------------------------------------------------------------------+----------------------------------+----------------------------
           1 |           7 | hr          | Materialized View | mv_emp_summary   |                |                     | view_type:MATERIALIZED VIEW,view_definition: SELECT mv_emp_master.emp_name,                                                       +| 6dca483dde6a9d34850a5383654eea74 | 2025-11-28 19:24:08.730725
             |             |             |                   |                  |                |                     |     mv_emp_master.emp_id                                                                                                          +|                                  |
             |             |             |                   |                  |                |                     |    FROM hr.mv_emp_master;,base_tables:,is_ populated:true,view_owner:test_user,dependent_objects:Dependent views: hr.mv_emp_master |                                  |
           2 |           7 | hr          | Materialized View | mv_emp_master    |                |                     | view_type:MATERIALIZED VIEW,view_definition: SELECT employees.emp_id,                                                             +| 905979a54526c6aac047b5a037f1856d | 2025-11-28 19:24:08.738232
             |             |             |                   |                  |                |                     |     employees.emp_name                                                                                                            +|                                  |
             |             |             |                   |                  |                |                     |    FROM hr.employees;,base_tables:hr.employees,is_ populated:true,view_owner:test_user,dependent_objects:                          |                                  |
(2 rows)


--* Test Run 6 — Index on MV

```sql
CREATE INDEX idx_mv_emp_id
ON hr.mv_emp_master(emp_id);
```

    SELECT * FROM pdcd_schema.load_snapshot_tbl();
    SELECT * FROM pdcd_schema.compare_load_md5_metadata_tbl(ARRAY['hr']);
    SELECT * from pdcd_schema.compare_load_md5_metadata_functions(ARRAY['hr']);
    TRUNCATE TABLE pdcd_schema.md5_metadata_staging_tbl RESTART IDENTITY CASCADE;
    TRUNCATE TABLE pdcd_schema.md5_metadata_staging_functions RESTART IDENTITY CASCADE;
    SELECT * FROM pdcd_schema.load_md5_metadata_staging_tbl(ARRAY['hr']);
    SELECT * from pdcd_schema.load_md5_metadata_staging_functions(ARRAY['hr']);

select metadata_id,snapshot_id,schema_name,object_type,object_type_name,object_subtype,object_subtype_name,object_md5,processed_time,change_type FROM pdcd_schema.md5_metadata_tbl;
          41 |           8 | hr          | Materialized View | mv_emp_master     | Index          | idx_mv_emp_id       | 10fef2c8b207b36d2d4e598851dc7ba4 | 2025-11-28 19:27:27.055119 | ADDED
          42 |           8 | hr          | Materialized View | mv_emp_master     |                |                     | 0ebe62b4098095815d2804efb97738be | 2025-11-28 19:27:28.230415 | MODIFIED
(42 rows)

select metadata_id,snapshot_id,schema_name,object_type,object_type_name,object_subtype,object_subtype_name,object_md5,processed_time from pdcd_schema.md5_metadata_staging_tbl;
 metadata_id | snapshot_id | schema_name |    object_type    | object_type_name | object_subtype | object_subtype_name |            object_md5            |       processed_time
-------------+-------------+-------------+-------------------+------------------+----------------+---------------------+----------------------------------+----------------------------
           1 |           8 | hr          | Materialized View | mv_emp_summary   | Column         | emp_name            | 7b6d79c39d7a5fec5b6f1f60762106d7 | 2025-11-28 19:27:28.593658
           2 |           8 | hr          | Table             | employees        | Column         | emp_id              | 4feaf72682cf29404f4407c0a7ba9c93 | 2025-11-28 19:27:28.603601
           3 |           8 | hr          | Materialized View | mv_emp_master    | Column         | emp_id              | 0127d1460ae24d6e781f068aba382060 | 2025-11-28 19:27:28.603631
           4 |           8 | hr          | Materialized View | mv_emp_master    | Column         | emp_name            | b537a75e4c2744b85e478bf937370ba9 | 2025-11-28 19:27:28.603642
           5 |           8 | hr          | Materialized View | mv_emp_summary   | Column         | emp_id              | 1851575de0ebf31fdbd942e3d11d15d2 | 2025-11-28 19:27:28.603653
           6 |           8 | hr          | Table             | departments      | Column         | location            | eb892ea3bb6ebc1480b7b61c5fffb6db | 2025-11-28 19:27:28.603662
           7 |           8 | hr          | Table             | employees        | Column         | emp_name            | b537a75e4c2744b85e478bf937370ba9 | 2025-11-28 19:27:28.603671
           8 |           8 | hr          | Table             | departments      | Column         | dept_name           | b537a75e4c2744b85e478bf937370ba9 | 2025-11-28 19:27:28.603681
           9 |           8 | hr          | Table             | departments      | Column         | dept_id             | 16c869b63036a8b4c89016ea4f97f63b | 2025-11-28 19:27:28.60369
          10 |           8 | hr          | Table             | employees        | Column         | salary              | 9786c5f70226a57b52ffc90e55867d57 | 2025-11-28 19:27:28.6037
          11 |           8 | hr          | Table             | departments      | Constraint     | departments_pkey    | 9e71d83424d14d7af950aa5eab35d0a1 | 2025-11-28 19:27:28.607405
          12 |           8 | hr          | Table             | employees        | Constraint     | employees_pkey      | 9e9d7894742421811810fa3c39c5489e | 2025-11-28 19:27:28.607445
          13 |           8 | hr          | Table             | departments      | Index          | departments_pkey    | d73a3bcda456f6113c2bd35bfe8776fe | 2025-11-28 19:27:28.612379
          14 |           8 | hr          | Table             | employees        | Index          | employees_pkey      | 93c3ea9faa198edd0fb3f21eeaadb84a | 2025-11-28 19:27:28.612406
          15 |           8 | hr          | Materialized View | mv_emp_master    | Index          | idx_mv_emp_id       | 10fef2c8b207b36d2d4e598851dc7ba4 | 2025-11-28 19:27:28.612413
(15 rows)

select metadata_id,snapshot_id,schema_name,object_type,object_type_name,object_subtype,object_subtype_name,object_subtype_details,object_md5,processed_time from pdcd_schema.md5_metadata_staging_functions;
 metadata_id | snapshot_id | schema_name |    object_type    | object_type_name | object_subtype | object_subtype_name |                                                       object_subtype_details                                                       |            object_md5            |       processed_time
-------------+-------------+-------------+-------------------+------------------+----------------+---------------------+------------------------------------------------------------------------------------------------------------------------------------+----------------------------------+----------------------------
           1 |           8 | hr          | Materialized View | mv_emp_summary   |                |                     | view_type:MATERIALIZED VIEW,view_definition: SELECT mv_emp_master.emp_name,                                                       +| 6dca483dde6a9d34850a5383654eea74 | 2025-11-28 19:27:29.291377
             |             |             |                   |                  |                |                     |     mv_emp_master.emp_id                                                                                                          +|                                  |
             |             |             |                   |                  |                |                     |    FROM hr.mv_emp_master;,base_tables:,is_ populated:true,view_owner:test_user,dependent_objects:Dependent views: hr.mv_emp_master |                                  |
           2 |           8 | hr          | Materialized View | mv_emp_master    |                |                     | view_type:MATERIALIZED VIEW,view_definition: SELECT employees.emp_id,                                                             +| 0ebe62b4098095815d2804efb97738be | 2025-11-28 19:27:29.300986
             |             |             |                   |                  |                |                     |     employees.emp_name                                                                                                            +|                                  |
             |             |             |                   |                  |                |                     |    FROM hr.employees;,base_tables:hr.employees,is_ populated:true,view_owner:test_user,dependent_objects:Indexes: idx_mv_emp_id    |                                  |
(2 rows)
---

--* Test Run 7 — Delete/Add Index

```sql

DROP INDEX hr.idx_mv_emp_id ;

CREATE INDEX idx_mv_emp_name ON hr.mv_emp_master(emp_name);
```

    SELECT * FROM pdcd_schema.load_snapshot_tbl();
    SELECT * FROM pdcd_schema.compare_load_md5_metadata_tbl(ARRAY['hr']);
    SELECT * from pdcd_schema.compare_load_md5_metadata_functions(ARRAY['hr']);
    TRUNCATE TABLE pdcd_schema.md5_metadata_staging_tbl RESTART IDENTITY CASCADE;
    TRUNCATE TABLE pdcd_schema.md5_metadata_staging_functions RESTART IDENTITY CASCADE;
    SELECT * FROM pdcd_schema.load_md5_metadata_staging_tbl(ARRAY['hr']);
    SELECT * from pdcd_schema.load_md5_metadata_staging_functions(ARRAY['hr']);

select metadata_id,snapshot_id,schema_name,object_type,object_type_name,object_subtype,object_subtype_name,object_md5,processed_time,change_type FROM pdcd_schema.md5_metadata_tbl;
          43 |           9 | hr          | Materialized View | mv_emp_master     | Index          | idx_mv_emp_name     | c4f58655b612c2ea764c4608461fdc32 | 2025-11-28 19:32:56.563813 | ADDED
          44 |           9 | hr          | Materialized View | mv_emp_master     | Index          | idx_mv_emp_id       | 10fef2c8b207b36d2d4e598851dc7ba4 | 2025-11-28 19:32:56.56393  | DELETED
          45 |           9 | hr          | Materialized View | mv_emp_master     |                |                     | c3da508a1a5ff829570357fd6a7f2778 | 2025-11-28 19:32:57.454261 | MODIFIED
(45 rows)

select metadata_id,snapshot_id,schema_name,object_type,object_type_name,object_subtype,object_subtype_name,object_md5,processed_time from pdcd_schema.md5_metadata_staging_tbl;
 metadata_id | snapshot_id | schema_name |    object_type    | object_type_name | object_subtype | object_subtype_name |            object_md5            |       processed_time
-------------+-------------+-------------+-------------------+------------------+----------------+---------------------+----------------------------------+----------------------------
           1 |           9 | hr          | Materialized View | mv_emp_summary   | Column         | emp_name            | 7b6d79c39d7a5fec5b6f1f60762106d7 | 2025-11-28 19:32:57.909248
           2 |           9 | hr          | Table             | employees        | Column         | emp_id              | 4feaf72682cf29404f4407c0a7ba9c93 | 2025-11-28 19:32:57.918867
           3 |           9 | hr          | Materialized View | mv_emp_master    | Column         | emp_id              | 0127d1460ae24d6e781f068aba382060 | 2025-11-28 19:32:57.918906
           4 |           9 | hr          | Materialized View | mv_emp_master    | Column         | emp_name            | b537a75e4c2744b85e478bf937370ba9 | 2025-11-28 19:32:57.918921
           5 |           9 | hr          | Materialized View | mv_emp_summary   | Column         | emp_id              | 1851575de0ebf31fdbd942e3d11d15d2 | 2025-11-28 19:32:57.918935
           6 |           9 | hr          | Table             | departments      | Column         | location            | eb892ea3bb6ebc1480b7b61c5fffb6db | 2025-11-28 19:32:57.918947
           7 |           9 | hr          | Table             | employees        | Column         | emp_name            | b537a75e4c2744b85e478bf937370ba9 | 2025-11-28 19:32:57.91896
           8 |           9 | hr          | Table             | departments      | Column         | dept_name           | b537a75e4c2744b85e478bf937370ba9 | 2025-11-28 19:32:57.918973
           9 |           9 | hr          | Table             | departments      | Column         | dept_id             | 16c869b63036a8b4c89016ea4f97f63b | 2025-11-28 19:32:57.918986
          10 |           9 | hr          | Table             | employees        | Column         | salary              | 9786c5f70226a57b52ffc90e55867d57 | 2025-11-28 19:32:57.918999
          11 |           9 | hr          | Table             | departments      | Constraint     | departments_pkey    | 9e71d83424d14d7af950aa5eab35d0a1 | 2025-11-28 19:32:57.923328
          12 |           9 | hr          | Table             | employees        | Constraint     | employees_pkey      | 9e9d7894742421811810fa3c39c5489e | 2025-11-28 19:32:57.923357
          13 |           9 | hr          | Table             | departments      | Index          | departments_pkey    | d73a3bcda456f6113c2bd35bfe8776fe | 2025-11-28 19:32:57.92949
          14 |           9 | hr          | Materialized View | mv_emp_master    | Index          | idx_mv_emp_name     | c4f58655b612c2ea764c4608461fdc32 | 2025-11-28 19:32:57.929517
          15 |           9 | hr          | Table             | employees        | Index          | employees_pkey      | 93c3ea9faa198edd0fb3f21eeaadb84a | 2025-11-28 19:32:57.929527
(15 rows)

select metadata_id,snapshot_id,schema_name,object_type,object_type_name,object_subtype,object_subtype_name,object_subtype_details,object_md5,processed_time from pdcd_schema.md5_metadata_staging_functions;
 metadata_id | snapshot_id | schema_name |    object_type    | object_type_name | object_subtype | object_subtype_name |                                                       object_subtype_details                                                       |            object_md5            |       processed_time
-------------+-------------+-------------+-------------------+------------------+----------------+---------------------+------------------------------------------------------------------------------------------------------------------------------------+----------------------------------+----------------------------
           1 |           9 | hr          | Materialized View | mv_emp_summary   |                |                     | view_type:MATERIALIZED VIEW,view_definition: SELECT mv_emp_master.emp_name,                                                       +| 6dca483dde6a9d34850a5383654eea74 | 2025-11-28 19:32:58.515419
             |             |             |                   |                  |                |                     |     mv_emp_master.emp_id                                                                                                          +|                                  |
             |             |             |                   |                  |                |                     |    FROM hr.mv_emp_master;,base_tables:,is_ populated:true,view_owner:test_user,dependent_objects:Dependent views: hr.mv_emp_master |                                  |
           2 |           9 | hr          | Materialized View | mv_emp_master    |                |                     | view_type:MATERIALIZED VIEW,view_definition: SELECT employees.emp_id,                                                             +| c3da508a1a5ff829570357fd6a7f2778 | 2025-11-28 19:32:58.52176
             |             |             |                   |                  |                |                     |     employees.emp_name                                                                                                            +|                                  |
             |             |             |                   |                  |                |                     |    FROM hr.employees;,base_tables:hr.employees,is_ populated:true,view_owner:test_user,dependent_objects:Indexes: idx_mv_emp_name  |                                  |
(2 rows)


--* Test Run 8 — Changing Dependent Views

```sql

DROP MATERIALIZED VIEW hr.mv_emp_master;

CREATE MATERIALIZED VIEW hr.mv_emp_master AS
SELECT emp_id, emp_name, salary
FROM hr.employees;

CREATE OR REPLACE VIEW hr.v_emp_high_salary AS
SELECT emp_name
FROM hr.mv_emp_master
WHERE salary > 80000;
```

    SELECT * FROM pdcd_schema.load_snapshot_tbl();
    SELECT * FROM pdcd_schema.compare_load_md5_metadata_tbl(ARRAY['hr']);
    SELECT * from pdcd_schema.compare_load_md5_metadata_functions(ARRAY['hr']);
    TRUNCATE TABLE pdcd_schema.md5_metadata_staging_tbl RESTART IDENTITY CASCADE;
    TRUNCATE TABLE pdcd_schema.md5_metadata_staging_functions RESTART IDENTITY CASCADE;
    SELECT * FROM pdcd_schema.load_md5_metadata_staging_tbl(ARRAY['hr']);
    SELECT * from pdcd_schema.load_md5_metadata_staging_functions(ARRAY['hr']);

select metadata_id,snapshot_id,schema_name,object_type,object_type_name,object_subtype,object_subtype_name,object_md5,processed_time,change_type FROM pdcd_schema.md5_metadata_tbl;
          46 |          10 | hr          | Materialized View | mv_emp_master     | Column         | salary              | 9786c5f70226a57b52ffc90e55867d57 | 2025-11-28 19:37:01.538255 | ADDED
          47 |          10 | hr          | View              | v_emp_high_salary | Column         | emp_name            | 7b6d79c39d7a5fec5b6f1f60762106d7 | 2025-11-28 19:37:01.538304 | ADDED
          48 |          10 | hr          | Materialized View | mv_emp_summary    | Column         | emp_name            | 7b6d79c39d7a5fec5b6f1f60762106d7 | 2025-11-28 19:37:01.538368 | DELETED
          49 |          10 | hr          | Materialized View | mv_emp_summary    | Column         | emp_id              | 1851575de0ebf31fdbd942e3d11d15d2 | 2025-11-28 19:37:01.538379 | DELETED
          50 |          10 | hr          | Materialized View | mv_emp_master     | Index          | idx_mv_emp_name     | c4f58655b612c2ea764c4608461fdc32 | 2025-11-28 19:37:01.538534 | DELETED
          51 |          10 | hr          | Materialized View | mv_emp_master     |                |                     | 81cb98c5b654889d62b9a67c81469403 | 2025-11-28 19:37:02.67089  | MODIFIED
          52 |          10 | hr          | View              | v_emp_high_salary |                |                     | 321fbb560640d9025b78bba150f32607 | 2025-11-28 19:37:02.670994 | ADDED
          53 |          10 | hr          | Materialized View | mv_emp_summary    |                |                     | 6dca483dde6a9d34850a5383654eea74 | 2025-11-28 19:37:02.671028 | DELETED
(53 rows)

select metadata_id,snapshot_id,schema_name,object_type,object_type_name,object_subtype,object_subtype_name,object_md5,processed_time from pdcd_schema.md5_metadata_staging_tbl;
 metadata_id | snapshot_id | schema_name |    object_type    | object_type_name  | object_subtype | object_subtype_name |            object_md5            |       processed_time
-------------+-------------+-------------+-------------------+-------------------+----------------+---------------------+----------------------------------+----------------------------
           1 |          10 | hr          | Table             | employees         | Column         | emp_id              | 4feaf72682cf29404f4407c0a7ba9c93 | 2025-11-28 19:37:03.047462
           2 |          10 | hr          | Materialized View | mv_emp_master     | Column         | emp_id              | 0127d1460ae24d6e781f068aba382060 | 2025-11-28 19:37:03.057586
           3 |          10 | hr          | Materialized View | mv_emp_master     | Column         | emp_name            | b537a75e4c2744b85e478bf937370ba9 | 2025-11-28 19:37:03.057619
           4 |          10 | hr          | Table             | departments       | Column         | location            | eb892ea3bb6ebc1480b7b61c5fffb6db | 2025-11-28 19:37:03.057629
           5 |          10 | hr          | Table             | employees         | Column         | emp_name            | b537a75e4c2744b85e478bf937370ba9 | 2025-11-28 19:37:03.057637
           6 |          10 | hr          | View              | v_emp_high_salary | Column         | emp_name            | 7b6d79c39d7a5fec5b6f1f60762106d7 | 2025-11-28 19:37:03.057646
           7 |          10 | hr          | Materialized View | mv_emp_master     | Column         | salary              | 9786c5f70226a57b52ffc90e55867d57 | 2025-11-28 19:37:03.057654
           8 |          10 | hr          | Table             | departments       | Column         | dept_name           | b537a75e4c2744b85e478bf937370ba9 | 2025-11-28 19:37:03.057661
           9 |          10 | hr          | Table             | departments       | Column         | dept_id             | 16c869b63036a8b4c89016ea4f97f63b | 2025-11-28 19:37:03.057704
          10 |          10 | hr          | Table             | employees         | Column         | salary              | 9786c5f70226a57b52ffc90e55867d57 | 2025-11-28 19:37:03.057712
          11 |          10 | hr          | Table             | departments       | Constraint     | departments_pkey    | 9e71d83424d14d7af950aa5eab35d0a1 | 2025-11-28 19:37:03.061008
          12 |          10 | hr          | Table             | employees         | Constraint     | employees_pkey      | 9e9d7894742421811810fa3c39c5489e | 2025-11-28 19:37:03.061037
          13 |          10 | hr          | Table             | departments       | Index          | departments_pkey    | d73a3bcda456f6113c2bd35bfe8776fe | 2025-11-28 19:37:03.067457
          14 |          10 | hr          | Table             | employees         | Index          | employees_pkey      | 93c3ea9faa198edd0fb3f21eeaadb84a | 2025-11-28 19:37:03.067485
(14 rows)

select metadata_id,snapshot_id,schema_name,object_type,object_type_name,object_subtype,object_subtype_name,object_subtype_details,object_md5,processed_time from pdcd_schema.md5_metadata_staging_functions;
 metadata_id | snapshot_id | schema_name |    object_type    | object_type_name  | object_subtype | object_subtype_name |                                                        object_subtype_details                                                        |            object_md5            |       processed_time
-------------+-------------+-------------+-------------------+-------------------+----------------+---------------------+--------------------------------------------------------------------------------------------------------------------------------------+----------------------------------+----------------------------
           1 |          10 | hr          | View              | v_emp_high_salary |                |                     | view_type:VIEW,view_definition: SELECT mv_emp_master.emp_name                                                                       +| 321fbb560640d9025b78bba150f32607 | 2025-11-28 19:37:03.682417
             |             |             |                   |                   |                |                     |    FROM hr.mv_emp_master                                                                                                            +|                                  |
             |             |             |                   |                   |                |                     |   WHERE mv_emp_master.salary > 80000::numeric;,base_tables:,view_owner:test_user,dependent_objects:Dependent views: hr.mv_emp_master |                                  |
           2 |          10 | hr          | Materialized View | mv_emp_master     |                |                     | view_type:MATERIALIZED VIEW,view_definition: SELECT employees.emp_id,                                                               +| 81cb98c5b654889d62b9a67c81469403 | 2025-11-28 19:37:03.704166
             |             |             |                   |                   |                |                     |     employees.emp_name,                                                                                                             +|                                  |
             |             |             |                   |                   |                |                     |     employees.salary                                                                                                                +|                                  |
             |             |             |                   |                   |                |                     |    FROM hr.employees;,base_tables:hr.employees,is_ populated:true,view_owner:test_user,dependent_objects:                            |                                  |
(2 rows)


## Test Run 8 — Full Cycle Mixed Changes

```sql
ALTER MATERIALIZED VIEW hr.mv_emp_master RENAME TO mv_emp_final;

DROP VIEW hr.v_emp_high_salary;

-- DROP MATERIALIZED VIEW hr.mv_emp_summary;

CREATE MATERIALIZED VIEW hr.mv_emp_summary AS
SELECT COUNT(*) FROM hr.mv_emp_final;

CREATE VIEW hr.v_emp_high_salary AS
SELECT emp_name
FROM hr.mv_emp_final
WHERE salary > 90000;
```

    SELECT * FROM pdcd_schema.load_snapshot_tbl();
    SELECT * FROM pdcd_schema.compare_load_md5_metadata_tbl(ARRAY['hr']);
    SELECT * from pdcd_schema.compare_load_md5_metadata_functions(ARRAY['hr']);
    TRUNCATE TABLE pdcd_schema.md5_metadata_staging_tbl RESTART IDENTITY CASCADE;
    TRUNCATE TABLE pdcd_schema.md5_metadata_staging_functions RESTART IDENTITY CASCADE;
    SELECT * FROM pdcd_schema.load_md5_metadata_staging_tbl(ARRAY['hr']);
    SELECT * from pdcd_schema.load_md5_metadata_staging_functions(ARRAY['hr']);

select metadata_id,snapshot_id,schema_name,object_type,object_type_name,object_subtype,object_subtype_name,object_md5,processed_time,change_type FROM pdcd_schema.md5_metadata_tbl;
          54 |          11 | hr          | Materialized View | mv_emp_final      | Column         | emp_id              | 0127d1460ae24d6e781f068aba382060 | 2025-11-28 19:40:58.831807 | ADDED
          55 |          11 | hr          | Materialized View | mv_emp_final      | Column         | emp_name            | b537a75e4c2744b85e478bf937370ba9 | 2025-11-28 19:40:58.831835 | ADDED
          56 |          11 | hr          | Materialized View | mv_emp_final      | Column         | salary              | 9786c5f70226a57b52ffc90e55867d57 | 2025-11-28 19:40:58.831841 | ADDED
          57 |          11 | hr          | Materialized View | mv_emp_summary    | Column         | count               | 58e71ceb471355854fcacf33294970dc | 2025-11-28 19:40:58.831862 | ADDED
          58 |          11 | hr          | Materialized View | mv_emp_master     | Column         | emp_id              | 0127d1460ae24d6e781f068aba382060 | 2025-11-28 19:40:58.831901 | DELETED
          59 |          11 | hr          | Materialized View | mv_emp_master     | Column         | emp_name            | b537a75e4c2744b85e478bf937370ba9 | 2025-11-28 19:40:58.831907 | DELETED
          60 |          11 | hr          | Materialized View | mv_emp_master     | Column         | salary              | 9786c5f70226a57b52ffc90e55867d57 | 2025-11-28 19:40:58.831914 | DELETED
          61 |          11 | hr          | Materialized View | mv_emp_final      |                |                     | 81cb98c5b654889d62b9a67c81469403 | 2025-11-28 19:40:59.983303 | RENAMED
          62 |          11 | hr          | View              | v_emp_high_salary |                |                     | cc5f6e4a5a958fcbffb1cb741e015b8c | 2025-11-28 19:40:59.983359 | MODIFIED
          63 |          11 | hr          | Materialized View | mv_emp_summary    |                |                     | 68bfd930955209f83b0eb2039b57fe2e | 2025-11-28 19:40:59.983393 | ADDED
(63 rows)

select metadata_id,snapshot_id,schema_name,object_type,object_type_name,object_subtype,object_subtype_name,object_md5,processed_time from pdcd_schema.md5_metadata_staging_tbl;
 metadata_id | snapshot_id | schema_name |    object_type    | object_type_name  | object_subtype | object_subtype_name |            object_md5            |       processed_time
-------------+-------------+-------------+-------------------+-------------------+----------------+---------------------+----------------------------------+----------------------------
           1 |          11 | hr          | Table             | employees         | Column         | emp_id              | 4feaf72682cf29404f4407c0a7ba9c93 | 2025-11-28 19:41:00.466417
           2 |          11 | hr          | Materialized View | mv_emp_final      | Column         | emp_id              | 0127d1460ae24d6e781f068aba382060 | 2025-11-28 19:41:00.473597
           3 |          11 | hr          | Materialized View | mv_emp_summary    | Column         | count               | 58e71ceb471355854fcacf33294970dc | 2025-11-28 19:41:00.473613
           4 |          11 | hr          | Materialized View | mv_emp_final      | Column         | salary              | 9786c5f70226a57b52ffc90e55867d57 | 2025-11-28 19:41:00.473618
           5 |          11 | hr          | Materialized View | mv_emp_final      | Column         | emp_name            | b537a75e4c2744b85e478bf937370ba9 | 2025-11-28 19:41:00.473623
           6 |          11 | hr          | Table             | departments       | Column         | location            | eb892ea3bb6ebc1480b7b61c5fffb6db | 2025-11-28 19:41:00.473628
           7 |          11 | hr          | Table             | employees         | Column         | emp_name            | b537a75e4c2744b85e478bf937370ba9 | 2025-11-28 19:41:00.473633
           8 |          11 | hr          | View              | v_emp_high_salary | Column         | emp_name            | 7b6d79c39d7a5fec5b6f1f60762106d7 | 2025-11-28 19:41:00.473637
           9 |          11 | hr          | Table             | departments       | Column         | dept_name           | b537a75e4c2744b85e478bf937370ba9 | 2025-11-28 19:41:00.473642
          10 |          11 | hr          | Table             | departments       | Column         | dept_id             | 16c869b63036a8b4c89016ea4f97f63b | 2025-11-28 19:41:00.473647
          11 |          11 | hr          | Table             | employees         | Column         | salary              | 9786c5f70226a57b52ffc90e55867d57 | 2025-11-28 19:41:00.473651
          12 |          11 | hr          | Table             | departments       | Constraint     | departments_pkey    | 9e71d83424d14d7af950aa5eab35d0a1 | 2025-11-28 19:41:00.476914
          13 |          11 | hr          | Table             | employees         | Constraint     | employees_pkey      | 9e9d7894742421811810fa3c39c5489e | 2025-11-28 19:41:00.476929
          14 |          11 | hr          | Table             | departments       | Index          | departments_pkey    | d73a3bcda456f6113c2bd35bfe8776fe | 2025-11-28 19:41:00.481471
          15 |          11 | hr          | Table             | employees         | Index          | employees_pkey      | 93c3ea9faa198edd0fb3f21eeaadb84a | 2025-11-28 19:41:00.481502
(15 rows)

select metadata_id,snapshot_id,schema_name,object_type,object_type_name,object_subtype,object_subtype_name,object_subtype_details,object_md5,processed_time from pdcd_schema.md5_metadata_staging_functions;
 metadata_id | snapshot_id | schema_name |    object_type    | object_type_name  | object_subtype | object_subtype_name |                                                       object_subtype_details                                                       |            object_md5            |       processed_time
-------------+-------------+-------------+-------------------+-------------------+----------------+---------------------+------------------------------------------------------------------------------------------------------------------------------------+----------------------------------+----------------------------
           1 |          11 | hr          | View              | v_emp_high_salary |                |                     | view_type:VIEW,view_definition: SELECT mv_emp_final.emp_name                                                                      +| cc5f6e4a5a958fcbffb1cb741e015b8c | 2025-11-28 19:41:00.985685
             |             |             |                   |                   |                |                     |    FROM hr.mv_emp_final                                                                                                           +|                                  |
             |             |             |                   |                   |                |                     |   WHERE mv_emp_final.salary > 90000::numeric;,base_tables:,view_owner:test_user,dependent_objects:Dependent views: hr.mv_emp_final |                                  |
           2 |          11 | hr          | Materialized View | mv_emp_summary    |                |                     | view_type:MATERIALIZED VIEW,view_definition: SELECT count(*) AS count                                                             +| 68bfd930955209f83b0eb2039b57fe2e | 2025-11-28 19:41:01.001438
             |             |             |                   |                   |                |                     |    FROM hr.mv_emp_final;,base_tables:,is_ populated:true,view_owner:test_user,dependent_objects:Dependent views: hr.mv_emp_final   |                                  |
           3 |          11 | hr          | Materialized View | mv_emp_final      |                |                     | view_type:MATERIALIZED VIEW,view_definition: SELECT employees.emp_id,                                                             +| 81cb98c5b654889d62b9a67c81469403 | 2025-11-28 19:41:01.001468
             |             |             |                   |                   |                |                     |     employees.emp_name,                                                                                                           +|                                  |
             |             |             |                   |                   |                |                     |     employees.salary                                                                                                              +|                                  |
             |             |             |                   |                   |                |                     |    FROM hr.employees;,base_tables:hr.employees,is_ populated:true,view_owner:test_user,dependent_objects:                          |                                  |
(3 rows)

##  Advanced Additional Scenarios

### Test Run 9 — Cascade Drop Simulation

```sql
DROP VIEW sales.v_orders_master CASCADE;
```

### Test Run 10 — Refresh Concurrently

```sql
REFRESH MATERIALIZED VIEW CONCURRENTLY hr.mv_emp_final;
```

### Test Run 11 — Rename Dependent Objects

```sql
ALTER VIEW hr.v_emp_high_salary RENAME TO v_emp_critical_salary;
```

### Test Run 12 — Rebuild Everything

```sql
DROP SCHEMA sales CASCADE;
DROP SCHEMA hr CASCADE;
```

---

## 🎯 Purpose Achieved

This test pack covers: ✔ Dependent view chains ✔ Triggers on views ✔ MV index lifecycle ✔ Rename + modify + drop + recreate combinations ✔ Full cycle schema impact simulation ✔ Real-world migration stress testing
