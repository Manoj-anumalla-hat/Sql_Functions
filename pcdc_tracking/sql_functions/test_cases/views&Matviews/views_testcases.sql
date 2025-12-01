# PostgreSQL Test Scenarios for Views & Materialized Views

This script provides a **complete structured test suite** for validating complex lifecycle changes on:

* Views (including dependent views & INSTEAD OF triggers)
* Materialized Views (including dependent views & indexes)

It simulates rename + modify + drop + add together to represent real production migration behaviour.

---

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

---

# 🔹 VIEW TEST SCENARIOS

SELECT * FROM pdcd_schema.load_snapshot_tbl();
SELECT * FROM pdcd_schema.load_md5_metadata_tbl(ARRAY['sales','hr']);
SELECT * FROM pdcd_schema.load_md5_metadata_staging_tbl(ARRAY['sales','hr']);
SELECT * from pdcd_schema.load_md5_metadata_staging_functions(ARRAY['sales','hr']);

select metadata_id,snapshot_id,schema_name,object_type,object_type_name,object_subtype,object_subtype_name,object_md5,processed_time,change_type FROM pdcd_schema.md5_metadata_tbl;
metadata_id | snapshot_id | schema_name | object_type | object_type_name | object_subtype | object_subtype_name |            object_md5            |       processed_time       | change_type
-------------+-------------+-------------+-------------+------------------+----------------+---------------------+----------------------------------+----------------------------+-------------
           1 |           1 | hr          | Table       | departments      | Column         | dept_id             | 94f0a420477141d95681142f0d0a58a7 | 2025-11-26 19:32:04.723189 | ADDED
           2 |           1 | hr          | Table       | departments      | Column         | dept_name           | b537a75e4c2744b85e478bf937370ba9 | 2025-11-26 19:32:04.726884 | ADDED
           3 |           1 | hr          | Table       | departments      | Column         | location            | eb892ea3bb6ebc1480b7b61c5fffb6db | 2025-11-26 19:32:04.726899 | ADDED
           4 |           1 | hr          | Table       | employees        | Column         | emp_id              | 4ce3c147aa23959741647e8e8b674144 | 2025-11-26 19:32:04.726926 | ADDED
           5 |           1 | hr          | Table       | employees        | Column         | emp_name            | b537a75e4c2744b85e478bf937370ba9 | 2025-11-26 19:32:04.726932 | ADDED
           6 |           1 | hr          | Table       | employees        | Column         | salary              | 9786c5f70226a57b52ffc90e55867d57 | 2025-11-26 19:32:04.726937 | ADDED
           7 |           1 | sales       | Table       | orders           | Column         | order_id            | b4a0f02b4504d1bb6a8914b4492d5605 | 2025-11-26 19:32:04.726941 | ADDED
           8 |           1 | sales       | Table       | orders           | Column         | customer_name       | b537a75e4c2744b85e478bf937370ba9 | 2025-11-26 19:32:04.726948 | ADDED
           9 |           1 | sales       | Table       | orders           | Column         | amount              | 9786c5f70226a57b52ffc90e55867d57 | 2025-11-26 19:32:04.726952 | ADDED
          10 |           1 | sales       | Table       | payments         | Column         | payment_id          | b9ef97ed1b56015241404938431b7501 | 2025-11-26 19:32:04.726957 | ADDED
          11 |           1 | sales       | Table       | payments         | Column         | order_id            | c1bd38510071b86246e457243c970262 | 2025-11-26 19:32:04.726962 | ADDED
          12 |           1 | sales       | Table       | payments         | Column         | payment_mode        | f37e89341863082d8eb1a9eaeb4eafce | 2025-11-26 19:32:04.726966 | ADDED
          13 |           1 | hr          | Table       | departments      | Constraint     | departments_pkey    | 9e71d83424d14d7af950aa5eab35d0a1 | 2025-11-26 19:32:04.72979  | ADDED
          14 |           1 | hr          | Table       | employees        | Constraint     | employees_pkey      | 9e9d7894742421811810fa3c39c5489e | 2025-11-26 19:32:04.729808 | ADDED
          15 |           1 | sales       | Table       | orders           | Constraint     | orders_pkey         | a1a2e253f7d5944d5aa77c169bf3395d | 2025-11-26 19:32:04.729813 | ADDED
          16 |           1 | sales       | Table       | payments         | Constraint     | payments_pkey       | 1cb904dee1b3a8d68462afbe0e742561 | 2025-11-26 19:32:04.72983  | ADDED
          17 |           1 | hr          | Table       | departments      | Index          | departments_pkey    | d73a3bcda456f6113c2bd35bfe8776fe | 2025-11-26 19:32:04.733254 | ADDED
          18 |           1 | hr          | Table       | employees        | Index          | employees_pkey      | 93c3ea9faa198edd0fb3f21eeaadb84a | 2025-11-26 19:32:04.733273 | ADDED
          19 |           1 | sales       | Table       | orders           | Index          | orders_pkey         | 0cecf5d617aede160696106c428d106a | 2025-11-26 19:32:04.733279 | ADDED
          20 |           1 | sales       | Table       | payments         | Index          | payments_pkey       | a66915faddc931836856b3749c75325b | 2025-11-26 19:32:04.733285 | ADDED
(20 rows)


select metadata_id,snapshot_id,schema_name,object_type,object_type_name,object_subtype,object_subtype_name,object_md5,processed_time from pdcd_schema.md5_metadata_staging_tbl;
-- same as above

select metadata_id,snapshot_id,schema_name,object_type,object_type_name,object_subtype,object_subtype_name,object_md5,processed_time from pdcd_schema.md5_metadata_staging_functions;

-- no functions,views,matviews created yet, so no output

## Base View

```sql
CREATE VIEW sales.v_orders_base AS
SELECT order_id, customer_name, amount
FROM sales.orders;
```

### Dependent View

```sql
CREATE VIEW sales.v_orders_report AS
SELECT order_id, customer_name
FROM sales.v_orders_base
WHERE amount > 1000;
```

---* Test Run 0 — Initial Load & MD5 Capture
    SELECT * FROM pdcd_schema.load_snapshot_tbl();
    SELECT * FROM pdcd_schema.compare_load_md5_metadata_tbl(ARRAY['sales','hr']);
    SELECT * from pdcd_schema.compare_load_md5_metadata_functions(ARRAY['sales','hr']);
    TRUNCATE TABLE pdcd_schema.md5_metadata_staging_tbl RESTART IDENTITY CASCADE;
    TRUNCATE TABLE pdcd_schema.md5_metadata_staging_functions RESTART IDENTITY CASCADE;
    SELECT * FROM pdcd_schema.load_md5_metadata_staging_tbl(ARRAY['sales','hr']);
    SELECT * from pdcd_schema.load_md5_metadata_staging_functions(ARRAY['sales','hr']);

select metadata_id,snapshot_id,schema_name,object_type,object_type_name,object_subtype,object_subtype_name,object_md5,processed_time,change_type FROM pdcd_schema.md5_metadata_tbl;
 metadata_id | snapshot_id | schema_name | object_type | object_type_name | object_subtype | object_subtype_name |            object_md5            |       processed_time       | change_type
          21 |           2 | sales       | Table       | v_orders_base    | Column         | order_id            | 3d8aeb78b1453edab2ecfd64244d4bbf | 2025-11-26 19:34:15.257795 | ADDED
          22 |           2 | sales       | Table       | v_orders_base    | Column         | customer_name       | b537a75e4c2744b85e478bf937370ba9 | 2025-11-26 19:34:15.257855 | ADDED
          23 |           2 | sales       | Table       | v_orders_base    | Column         | amount              | 9786c5f70226a57b52ffc90e55867d57 | 2025-11-26 19:34:15.257864 | ADDED
          24 |           2 | sales       | Table       | v_orders_report  | Column         | order_id            | 3d8aeb78b1453edab2ecfd64244d4bbf | 2025-11-26 19:34:15.25787  | ADDED
          25 |           2 | sales       | Table       | v_orders_report  | Column         | customer_name       | b537a75e4c2744b85e478bf937370ba9 | 2025-11-26 19:34:15.257875 | ADDED
          26 |           2 | sales       | View        | v_orders_base    |                |                     | 1e6b3547c6b4cb323d9f1e21d4ecec0f | 2025-11-26 19:34:16.231075 | ADDED
          27 |           2 | sales       | View        | v_orders_report  |                |                     | c09fcd23b41c82a278650e57f84d7dd3 | 2025-11-26 19:34:16.231148 | ADDED
(27 rows)

select metadata_id,snapshot_id,schema_name,object_type,object_type_name,object_subtype,object_subtype_name,object_md5,processed_time from pdcd_schema.md5_metadata_staging_tbl;
 metadata_id | snapshot_id | schema_name | object_type | object_type_name | object_subtype | object_subtype_name |            object_md5            |       processed_time
-------------+-------------+-------------+-------------+------------------+----------------+---------------------+----------------------------------+----------------------------
           1 |           2 | sales       | Table       | v_orders_report  | Column         | order_id            | 3d8aeb78b1453edab2ecfd64244d4bbf | 2025-11-26 19:34:16.988501
           2 |           2 | sales       | Table       | v_orders_report  | Column         | customer_name       | b537a75e4c2744b85e478bf937370ba9 | 2025-11-26 19:34:16.996036
           3 |           2 | sales       | Table       | payments         | Column         | payment_id          | b9ef97ed1b56015241404938431b7501 | 2025-11-26 19:34:16.996056
           4 |           2 | hr          | Table       | employees        | Column         | emp_id              | 4ce3c147aa23959741647e8e8b674144 | 2025-11-26 19:34:16.996061
           5 |           2 | sales       | Table       | v_orders_base    | Column         | order_id            | 3d8aeb78b1453edab2ecfd64244d4bbf | 2025-11-26 19:34:16.996066
           6 |           2 | hr          | Table       | departments      | Column         | location            | eb892ea3bb6ebc1480b7b61c5fffb6db | 2025-11-26 19:34:16.996069
           7 |           2 | sales       | Table       | orders           | Column         | amount              | 9786c5f70226a57b52ffc90e55867d57 | 2025-11-26 19:34:16.996074
           8 |           2 | hr          | Table       | employees        | Column         | emp_name            | b537a75e4c2744b85e478bf937370ba9 | 2025-11-26 19:34:16.996078
           9 |           2 | sales       | Table       | payments         | Column         | payment_mode        | f37e89341863082d8eb1a9eaeb4eafce | 2025-11-26 19:34:16.996082
          10 |           2 | sales       | Table       | orders           | Column         | order_id            | b4a0f02b4504d1bb6a8914b4492d5605 | 2025-11-26 19:34:16.996086
          11 |           2 | sales       | Table       | v_orders_base    | Column         | amount              | 9786c5f70226a57b52ffc90e55867d57 | 2025-11-26 19:34:16.996089
          12 |           2 | hr          | Table       | departments      | Column         | dept_name           | b537a75e4c2744b85e478bf937370ba9 | 2025-11-26 19:34:16.996093
          13 |           2 | sales       | Table       | payments         | Column         | order_id            | c1bd38510071b86246e457243c970262 | 2025-11-26 19:34:16.996097
          14 |           2 | sales       | Table       | orders           | Column         | customer_name       | b537a75e4c2744b85e478bf937370ba9 | 2025-11-26 19:34:16.996101
          15 |           2 | sales       | Table       | v_orders_base    | Column         | customer_name       | b537a75e4c2744b85e478bf937370ba9 | 2025-11-26 19:34:16.996105
          16 |           2 | hr          | Table       | employees        | Column         | salary              | 9786c5f70226a57b52ffc90e55867d57 | 2025-11-26 19:34:16.996108
          17 |           2 | hr          | Table       | departments      | Column         | dept_id             | 94f0a420477141d95681142f0d0a58a7 | 2025-11-26 19:34:16.996113
          18 |           2 | sales       | Table       | orders           | Constraint     | orders_pkey         | a1a2e253f7d5944d5aa77c169bf3395d | 2025-11-26 19:34:16.998108
          19 |           2 | sales       | Table       | payments         | Constraint     | payments_pkey       | 1cb904dee1b3a8d68462afbe0e742561 | 2025-11-26 19:34:16.998121
          20 |           2 | hr          | Table       | departments      | Constraint     | departments_pkey    | 9e71d83424d14d7af950aa5eab35d0a1 | 2025-11-26 19:34:16.998129
          21 |           2 | hr          | Table       | employees        | Constraint     | employees_pkey      | 9e9d7894742421811810fa3c39c5489e | 2025-11-26 19:34:16.998135
          22 |           2 | hr          | Table       | departments      | Index          | departments_pkey    | d73a3bcda456f6113c2bd35bfe8776fe | 2025-11-26 19:34:17.001099
          23 |           2 | sales       | Table       | orders           | Index          | orders_pkey         | 0cecf5d617aede160696106c428d106a | 2025-11-26 19:34:17.001109
          24 |           2 | sales       | Table       | payments         | Index          | payments_pkey       | a66915faddc931836856b3749c75325b | 2025-11-26 19:34:17.001115
          25 |           2 | hr          | Table       | employees        | Index          | employees_pkey      | 93c3ea9faa198edd0fb3f21eeaadb84a | 2025-11-26 19:34:17.001119
(25 rows)


select metadata_id,snapshot_id,schema_name,object_type,object_type_name,object_subtype,object_subtype_name,object_md5,processed_time from pdcd_schema.md5_metadata_staging_functions;
 metadata_id | snapshot_id | schema_name | object_type | object_type_name | object_subtype | object_subtype_name |            object_md5            |       processed_time
-------------+-------------+-------------+-------------+------------------+----------------+---------------------+----------------------------------+----------------------------
           1 |           2 | sales       | View        | v_orders_base    |                |                     | 1e6b3547c6b4cb323d9f1e21d4ecec0f | 2025-11-26 19:34:17.788329
           2 |           2 | sales       | View        | v_orders_report  |                |                     | c09fcd23b41c82a278650e57f84d7dd3 | 2025-11-26 19:34:17.795484
(2 rows)

---

 -- * Test Run 1 — Initial Additions & Modifications

```sql
CREATE OR REPLACE VIEW sales.v_orders_base AS
SELECT order_id, customer_name, amount
FROM sales.orders
WHERE amount IS NOT NULL;
```
    SELECT * FROM pdcd_schema.load_snapshot_tbl();
    SELECT * FROM pdcd_schema.compare_load_md5_metadata_tbl(ARRAY['sales','hr']);
    SELECT * from pdcd_schema.compare_load_md5_metadata_functions(ARRAY['sales','hr']);
    TRUNCATE TABLE pdcd_schema.md5_metadata_staging_tbl RESTART IDENTITY CASCADE;
    TRUNCATE TABLE pdcd_schema.md5_metadata_staging_functions RESTART IDENTITY CASCADE;
    SELECT * FROM pdcd_schema.load_md5_metadata_staging_tbl(ARRAY['sales','hr']);
    SELECT * from pdcd_schema.load_md5_metadata_staging_functions(ARRAY['sales','hr']);

select metadata_id,snapshot_id,schema_name,object_type,object_type_name,object_subtype,object_subtype_name,object_md5,processed_time,change_type FROM pdcd_schema.md5_metadata_tbl;
metadata_id | snapshot_id | schema_name | object_type | object_type_name | object_subtype | object_subtype_name |            object_md5            |       processed_time       | change_type
         28 |           3 | sales       | View        | v_orders_base    |                |                     | 8ab5ff64b11f49649311e714ba77d48c | 2025-11-26 19:38:25.416182 | MODIFIED
(28 rows)

select metadata_id,snapshot_id,schema_name,object_type,object_type_name,object_subtype,object_subtype_name,object_md5,processed_time from pdcd_schema.md5_metadata_staging_tbl;
-- same as above

select metadata_id,snapshot_id,schema_name,object_type,object_type_name,object_subtype,object_subtype_name,object_subtype_details,object_md5,processed_time from pdcd_schema.md5_metadata_staging_functions;
 metadata_id | snapshot_id | schema_name | object_type | object_type_name | object_subtype | object_subtype_name |                                                         object_subtype_details                                                         |            object_md5            |       processed_time
-------------+-------------+-------------+-------------+------------------+----------------+---------------------+----------------------------------------------------------------------------------------------------------------------------------------+----------------------------------+----------------------------
           1 |           3 | sales       | View        | v_orders_base    |                |                     | view_type:VIEW,view_definition: SELECT orders.order_id,                                                                               +| 8ab5ff64b11f49649311e714ba77d48c | 2025-11-26 19:38:26.825567
             |             |             |             |                  |                |                     |     orders.customer_name,                                                                                                             +|                                  |
             |             |             |             |                  |                |                     |     orders.amount                                                                                                                     +|                                  |
             |             |             |             |                  |                |                     |    FROM sales.orders                                                                                                                  +|                                  |
             |             |             |             |                  |                |                     |   WHERE orders.amount IS NOT NULL;,base_tables:sales.orders,view_owner:test_user,dependent_objects:                                    |                                  |
           2 |           3 | sales       | View        | v_orders_report  |                |                     | view_type:VIEW,view_definition: SELECT v_orders_base.order_id,                                                                        +| c09fcd23b41c82a278650e57f84d7dd3 | 2025-11-26 19:38:26.834019
             |             |             |             |                  |                |                     |     v_orders_base.customer_name                                                                                                       +|                                  |
             |             |             |             |                  |                |                     |    FROM sales.v_orders_base                                                                                                           +|                                  |
             |             |             |             |                  |                |                     |   WHERE v_orders_base.amount > 1000::numeric;,base_tables:,view_owner:test_user,dependent_objects:Dependent views: sales.v_orders_base |                                  |
(2 rows)
---

--* Test Run 2 — Renaming and Adding Views

```sql
ALTER VIEW sales.v_orders_base RENAME TO v_orders_master;

CREATE VIEW sales.v_orders_summary AS
SELECT customer_name, SUM(amount) AS total_amount
FROM sales.v_orders_master
GROUP BY customer_name;
```

    SELECT * FROM pdcd_schema.load_snapshot_tbl();
    SELECT * FROM pdcd_schema.compare_load_md5_metadata_tbl(ARRAY['sales','hr']);
    SELECT * from pdcd_schema.compare_load_md5_metadata_functions(ARRAY['sales','hr']);
    TRUNCATE TABLE pdcd_schema.md5_metadata_staging_tbl RESTART IDENTITY CASCADE;
    TRUNCATE TABLE pdcd_schema.md5_metadata_staging_functions RESTART IDENTITY CASCADE;
    SELECT * FROM pdcd_schema.load_md5_metadata_staging_tbl(ARRAY['sales','hr']);
    SELECT * from pdcd_schema.load_md5_metadata_staging_functions(ARRAY['sales','hr']);

select metadata_id,snapshot_id,schema_name,object_type,object_type_name,object_subtype,object_subtype_name,object_md5,processed_time,change_type FROM pdcd_schema.md5_metadata_tbl;
          29 |           4 | sales       | Table       | v_orders_master  | Column         | order_id            | 3d8aeb78b1453edab2ecfd64244d4bbf | 2025-11-26 19:42:28.601694 | ADDED
          30 |           4 | sales       | Table       | v_orders_master  | Column         | customer_name       | b537a75e4c2744b85e478bf937370ba9 | 2025-11-26 19:42:28.60172  | ADDED
          31 |           4 | sales       | Table       | v_orders_master  | Column         | amount              | 9786c5f70226a57b52ffc90e55867d57 | 2025-11-26 19:42:28.601723 | ADDED
          32 |           4 | sales       | Table       | v_orders_summary | Column         | customer_name       | 7b6d79c39d7a5fec5b6f1f60762106d7 | 2025-11-26 19:42:28.601726 | ADDED
          33 |           4 | sales       | Table       | v_orders_summary | Column         | total_amount        | 8eca8756e1ba7d796bdd2e700d366d80 | 2025-11-26 19:42:28.601729 | ADDED
          34 |           4 | sales       | Table       | v_orders_base    | Column         | order_id            | 3d8aeb78b1453edab2ecfd64244d4bbf | 2025-11-26 19:42:28.601754 | DELETED
          35 |           4 | sales       | Table       | v_orders_base    | Column         | amount              | 9786c5f70226a57b52ffc90e55867d57 | 2025-11-26 19:42:28.601759 | DELETED
          36 |           4 | sales       | Table       | v_orders_base    | Column         | customer_name       | b537a75e4c2744b85e478bf937370ba9 | 2025-11-26 19:42:28.601762 | DELETED
          37 |           4 | sales       | View        | v_orders_master  |                |                     | 8ab5ff64b11f49649311e714ba77d48c | 2025-11-26 19:42:29.247575 | RENAMED
          38 |           4 | sales       | View        | v_orders_report  |                |                     | 3159b2156eabc1093877d3d0e02b18fa | 2025-11-26 19:42:29.251608 | MODIFIED
          39 |           4 | sales       | View        | v_orders_summary |                |                     | 4c38339a55375263fe79b7180a3f5fad | 2025-11-26 19:42:29.251683 | ADDED
(39 rows)

select metadata_id,snapshot_id,schema_name,object_type,object_type_name,object_subtype,object_subtype_name,object_md5,processed_time from pdcd_schema.md5_metadata_staging_tbl;

--


select metadata_id,snapshot_id,schema_name,object_type,object_type_name,object_subtype,object_subtype_name,object_subtype_details,object_md5,processed_time from pdcd_schema.md5_metadata_staging_functions;
metadata_id | snapshot_id | schema_name | object_type | object_type_name | object_subtype | object_subtype_name |                                                           object_subtype_details                                                           |            object_md5            |       processed_time
-------------+-------------+-------------+-------------+------------------+----------------+---------------------+--------------------------------------------------------------------------------------------------------------------------------------------+----------------------------------+----------------------------
           1 |           4 | sales       | View        | v_orders_report  |                |                     | view_type:VIEW,view_definition: SELECT v_orders_master.order_id,                                                                          +| 3159b2156eabc1093877d3d0e02b18fa | 2025-11-26 19:42:30.522219
             |             |             |             |                  |                |                     |     v_orders_master.customer_name                                                                                                         +|                                  |
             |             |             |             |                  |                |                     |    FROM sales.v_orders_master                                                                                                             +|                                  |
             |             |             |             |                  |                |                     |   WHERE v_orders_master.amount > 1000::numeric;,base_tables:,view_owner:test_user,dependent_objects:Dependent views: sales.v_orders_master |                                  |
           2 |           4 | sales       | View        | v_orders_master  |                |                     | view_type:VIEW,view_definition: SELECT orders.order_id,                                                                                   +| 8ab5ff64b11f49649311e714ba77d48c | 2025-11-26 19:42:30.52956
             |             |             |             |                  |                |                     |     orders.customer_name,                                                                                                                 +|                                  |
             |             |             |             |                  |                |                     |     orders.amount                                                                                                                         +|                                  |
             |             |             |             |                  |                |                     |    FROM sales.orders                                                                                                                      +|                                  |
             |             |             |             |                  |                |                     |   WHERE orders.amount IS NOT NULL;,base_tables:sales.orders,view_owner:test_user,dependent_objects:                                        |                                  |
           3 |           4 | sales       | View        | v_orders_summary |                |                     | view_type:VIEW,view_definition: SELECT v_orders_master.customer_name,                                                                     +| 4c38339a55375263fe79b7180a3f5fad | 2025-11-26 19:42:30.529582
             |             |             |             |                  |                |                     |     sum(v_orders_master.amount) AS total_amount                                                                                           +|                                  |
             |             |             |             |                  |                |                     |    FROM sales.v_orders_master                                                                                                             +|                                  |
             |             |             |             |                  |                |                     |   GROUP BY v_orders_master.customer_name;,base_tables:,view_owner:test_user,dependent_objects:Dependent views: sales.v_orders_master       |                                  |
(3 rows)

--* Test Run 3 — View Definition Changes

DROP VIEW sales.v_orders_master Cascade; -- to change definition, we need to drop dependent views first and recreate the same view by changing definition.

```sql
CREATE OR REPLACE VIEW sales.v_orders_master AS
SELECT order_id, customer_name
FROM sales.orders;
```
    SELECT * FROM pdcd_schema.load_snapshot_tbl();
    SELECT * FROM pdcd_schema.compare_load_md5_metadata_tbl(ARRAY['sales','hr']);
    SELECT * from pdcd_schema.compare_load_md5_metadata_functions(ARRAY['sales','hr']);
    TRUNCATE TABLE pdcd_schema.md5_metadata_staging_tbl RESTART IDENTITY CASCADE;
    TRUNCATE TABLE pdcd_schema.md5_metadata_staging_functions RESTART IDENTITY CASCADE;
    SELECT * FROM pdcd_schema.load_md5_metadata_staging_tbl(ARRAY['sales','hr']);
    SELECT * from pdcd_schema.load_md5_metadata_staging_functions(ARRAY['sales','hr']);

select metadata_id,snapshot_id,schema_name,object_type,object_type_name,object_subtype,object_subtype_name,object_md5,processed_time,change_type FROM pdcd_schema.md5_metadata_tbl;
          40 |           5 | sales       | Table       | v_orders_report  | Column         | order_id            | 3d8aeb78b1453edab2ecfd64244d4bbf | 2025-11-26 20:36:06.183791 | DELETED
          41 |           5 | sales       | Table       | v_orders_report  | Column         | customer_name       | b537a75e4c2744b85e478bf937370ba9 | 2025-11-26 20:36:06.183848 | DELETED
          42 |           5 | sales       | Table       | v_orders_summary | Column         | customer_name       | 7b6d79c39d7a5fec5b6f1f60762106d7 | 2025-11-26 20:36:06.183855 | DELETED
          43 |           5 | sales       | Table       | v_orders_master  | Column         | amount              | 9786c5f70226a57b52ffc90e55867d57 | 2025-11-26 20:36:06.183862 | DELETED
          44 |           5 | sales       | Table       | v_orders_summary | Column         | total_amount        | 8eca8756e1ba7d796bdd2e700d366d80 | 2025-11-26 20:36:06.183866 | DELETED
          45 |           5 | sales       | View        | v_orders_master  |                |                     | 8ade1ed7191ba1ff31a0d75a16c67096 | 2025-11-26 20:36:07.2273   | MODIFIED
          46 |           5 | sales       | View        | v_orders_report  |                |                     | 3159b2156eabc1093877d3d0e02b18fa | 2025-11-26 20:36:07.227487 | DELETED
          47 |           5 | sales       | View        | v_orders_summary |                |                     | 4c38339a55375263fe79b7180a3f5fad | 2025-11-26 20:36:07.227512 | DELETED
(47 rows)

select metadata_id,snapshot_id,schema_name,object_type,object_type_name,object_subtype,object_subtype_name,object_md5,processed_time from pdcd_schema.md5_metadata_staging_tbl;
 metadata_id | snapshot_id | schema_name | object_type | object_type_name | object_subtype | object_subtype_name |            object_md5            |       processed_time
-------------+-------------+-------------+-------------+------------------+----------------+---------------------+----------------------------------+----------------------------
           1 |           5 | sales       | Table       | payments         | Column         | payment_id          | b9ef97ed1b56015241404938431b7501 | 2025-11-26 20:36:07.754666
           2 |           5 | sales       | Table       | v_orders_master  | Column         | order_id            | 3d8aeb78b1453edab2ecfd64244d4bbf | 2025-11-26 20:36:07.759436
           3 |           5 | hr          | Table       | employees        | Column         | emp_id              | 4ce3c147aa23959741647e8e8b674144 | 2025-11-26 20:36:07.75945
           4 |           5 | hr          | Table       | departments      | Column         | location            | eb892ea3bb6ebc1480b7b61c5fffb6db | 2025-11-26 20:36:07.759466
           5 |           5 | sales       | Table       | orders           | Column         | amount              | 9786c5f70226a57b52ffc90e55867d57 | 2025-11-26 20:36:07.759472
           6 |           5 | hr          | Table       | employees        | Column         | emp_name            | b537a75e4c2744b85e478bf937370ba9 | 2025-11-26 20:36:07.759478
           7 |           5 | sales       | Table       | payments         | Column         | payment_mode        | f37e89341863082d8eb1a9eaeb4eafce | 2025-11-26 20:36:07.759484
           8 |           5 | sales       | Table       | orders           | Column         | order_id            | b4a0f02b4504d1bb6a8914b4492d5605 | 2025-11-26 20:36:07.75949
           9 |           5 | sales       | Table       | v_orders_master  | Column         | customer_name       | b537a75e4c2744b85e478bf937370ba9 | 2025-11-26 20:36:07.759495
          10 |           5 | hr          | Table       | departments      | Column         | dept_name           | b537a75e4c2744b85e478bf937370ba9 | 2025-11-26 20:36:07.759502
          11 |           5 | sales       | Table       | payments         | Column         | order_id            | c1bd38510071b86246e457243c970262 | 2025-11-26 20:36:07.759508
          12 |           5 | sales       | Table       | orders           | Column         | customer_name       | b537a75e4c2744b85e478bf937370ba9 | 2025-11-26 20:36:07.759513
          13 |           5 | hr          | Table       | employees        | Column         | salary              | 9786c5f70226a57b52ffc90e55867d57 | 2025-11-26 20:36:07.759519
          14 |           5 | hr          | Table       | departments      | Column         | dept_id             | 94f0a420477141d95681142f0d0a58a7 | 2025-11-26 20:36:07.759525
          15 |           5 | sales       | Table       | orders           | Constraint     | orders_pkey         | a1a2e253f7d5944d5aa77c169bf3395d | 2025-11-26 20:36:07.761716
          16 |           5 | sales       | Table       | payments         | Constraint     | payments_pkey       | 1cb904dee1b3a8d68462afbe0e742561 | 2025-11-26 20:36:07.761729
          17 |           5 | hr          | Table       | departments      | Constraint     | departments_pkey    | 9e71d83424d14d7af950aa5eab35d0a1 | 2025-11-26 20:36:07.761735
          18 |           5 | hr          | Table       | employees        | Constraint     | employees_pkey      | 9e9d7894742421811810fa3c39c5489e | 2025-11-26 20:36:07.761741
          19 |           5 | hr          | Table       | departments      | Index          | departments_pkey    | d73a3bcda456f6113c2bd35bfe8776fe | 2025-11-26 20:36:07.764968
          20 |           5 | sales       | Table       | orders           | Index          | orders_pkey         | 0cecf5d617aede160696106c428d106a | 2025-11-26 20:36:07.76498
          21 |           5 | sales       | Table       | payments         | Index          | payments_pkey       | a66915faddc931836856b3749c75325b | 2025-11-26 20:36:07.764986
          22 |           5 | hr          | Table       | employees        | Index          | employees_pkey      | 93c3ea9faa198edd0fb3f21eeaadb84a | 2025-11-26 20:36:07.764991
(22 rows)

select metadata_id,snapshot_id,schema_name,object_type,object_type_name,object_subtype,object_subtype_name,object_subtype_details,object_md5,processed_time from pdcd_schema.md5_metadata_staging_functions;
 
 metadata_id | snapshot_id | schema_name | object_type | object_type_name | object_subtype | object_subtype_name |                                 object_subtype_details                                 |            object_md5            |       processed_time
-------------+-------------+-------------+-------------+------------------+----------------+---------------------+----------------------------------------------------------------------------------------+----------------------------------+----------------------------
           1 |           5 | sales       | View        | v_orders_master  |                |                     | view_type:VIEW,view_definition: SELECT orders.order_id,                               +| 8ade1ed7191ba1ff31a0d75a16c67096 | 2025-11-26 20:36:08.643821
             |             |             |             |                  |                |                     |     orders.customer_name                                                              +|                                  |
             |             |             |             |                  |                |                     |    FROM sales.orders;,base_tables:sales.orders,view_owner:test_user,dependent_objects: |                                  |
(1 row)

---

--* Test Run 4 — Dropping and Adding Back Views

```sql
DROP VIEW sales.v_orders_summary;

CREATE VIEW sales.v_orders_summary AS
SELECT customer_name, COUNT(order_id) AS total_orders
FROM sales.v_orders_master
GROUP BY customer_name;
```

    SELECT * FROM pdcd_schema.load_snapshot_tbl();
    SELECT * FROM pdcd_schema.compare_load_md5_metadata_tbl(ARRAY['sales','hr']);
    SELECT * from pdcd_schema.compare_load_md5_metadata_functions(ARRAY['sales','hr']);
    TRUNCATE TABLE pdcd_schema.md5_metadata_staging_tbl RESTART IDENTITY CASCADE;
    TRUNCATE TABLE pdcd_schema.md5_metadata_staging_functions RESTART IDENTITY CASCADE;
    SELECT * FROM pdcd_schema.load_md5_metadata_staging_tbl(ARRAY['sales','hr']);
    SELECT * from pdcd_schema.load_md5_metadata_staging_functions(ARRAY['sales','hr']);

select metadata_id,snapshot_id,schema_name,object_type,object_type_name,object_subtype,object_subtype_name,object_md5,processed_time,change_type FROM pdcd_schema.md5_metadata_tbl;
          48 |           6 | sales       | Table       | v_orders_summary | Column         | customer_name       | 7b6d79c39d7a5fec5b6f1f60762106d7 | 2025-11-26 20:44:32.233848 | ADDED
          49 |           6 | sales       | Table       | v_orders_summary | Column         | total_orders        | 281220c8e13d3604037c4cb48f84b433 | 2025-11-26 20:44:32.23391  | ADDED
          50 |           6 | sales       | View        | v_orders_summary |                |                     | 00f9b1830b32611113eb4bd8bf039d04 | 2025-11-26 20:44:34.512271 | ADDED
(50 rows)

select metadata_id,snapshot_id,schema_name,object_type,object_type_name,object_subtype,object_subtype_name,object_md5,processed_time from pdcd_schema.md5_metadata_staging_tbl;
 metadata_id | snapshot_id | schema_name | object_type | object_type_name | object_subtype | object_subtype_name |            object_md5            |       processed_time
-------------+-------------+-------------+-------------+------------------+----------------+---------------------+----------------------------------+----------------------------
           1 |           6 | sales       | Table       | payments         | Column         | payment_id          | b9ef97ed1b56015241404938431b7501 | 2025-11-26 20:44:35.00102
           2 |           6 | sales       | Table       | v_orders_master  | Column         | order_id            | 3d8aeb78b1453edab2ecfd64244d4bbf | 2025-11-26 20:44:35.009391
           3 |           6 | sales       | Table       | v_orders_summary | Column         | customer_name       | 7b6d79c39d7a5fec5b6f1f60762106d7 | 2025-11-26 20:44:35.009418
           4 |           6 | hr          | Table       | employees        | Column         | emp_id              | 4ce3c147aa23959741647e8e8b674144 | 2025-11-26 20:44:35.009426
           5 |           6 | hr          | Table       | departments      | Column         | location            | eb892ea3bb6ebc1480b7b61c5fffb6db | 2025-11-26 20:44:35.009433
           6 |           6 | sales       | Table       | orders           | Column         | amount              | 9786c5f70226a57b52ffc90e55867d57 | 2025-11-26 20:44:35.00944
           7 |           6 | hr          | Table       | employees        | Column         | emp_name            | b537a75e4c2744b85e478bf937370ba9 | 2025-11-26 20:44:35.009447
           8 |           6 | sales       | Table       | payments         | Column         | payment_mode        | f37e89341863082d8eb1a9eaeb4eafce | 2025-11-26 20:44:35.009453
           9 |           6 | sales       | Table       | orders           | Column         | order_id            | b4a0f02b4504d1bb6a8914b4492d5605 | 2025-11-26 20:44:35.00946
          10 |           6 | sales       | Table       | v_orders_master  | Column         | customer_name       | b537a75e4c2744b85e478bf937370ba9 | 2025-11-26 20:44:35.009467
          11 |           6 | hr          | Table       | departments      | Column         | dept_name           | b537a75e4c2744b85e478bf937370ba9 | 2025-11-26 20:44:35.009473
          12 |           6 | sales       | Table       | payments         | Column         | order_id            | c1bd38510071b86246e457243c970262 | 2025-11-26 20:44:35.009479
          13 |           6 | sales       | Table       | orders           | Column         | customer_name       | b537a75e4c2744b85e478bf937370ba9 | 2025-11-26 20:44:35.009485
          14 |           6 | sales       | Table       | v_orders_summary | Column         | total_orders        | 281220c8e13d3604037c4cb48f84b433 | 2025-11-26 20:44:35.009491
          15 |           6 | hr          | Table       | employees        | Column         | salary              | 9786c5f70226a57b52ffc90e55867d57 | 2025-11-26 20:44:35.009498
          16 |           6 | hr          | Table       | departments      | Column         | dept_id             | 94f0a420477141d95681142f0d0a58a7 | 2025-11-26 20:44:35.009505
          17 |           6 | sales       | Table       | orders           | Constraint     | orders_pkey         | a1a2e253f7d5944d5aa77c169bf3395d | 2025-11-26 20:44:35.01217
          18 |           6 | sales       | Table       | payments         | Constraint     | payments_pkey       | 1cb904dee1b3a8d68462afbe0e742561 | 2025-11-26 20:44:35.012188
          19 |           6 | hr          | Table       | departments      | Constraint     | departments_pkey    | 9e71d83424d14d7af950aa5eab35d0a1 | 2025-11-26 20:44:35.012194
          20 |           6 | hr          | Table       | employees        | Constraint     | employees_pkey      | 9e9d7894742421811810fa3c39c5489e | 2025-11-26 20:44:35.012199
          21 |           6 | hr          | Table       | departments      | Index          | departments_pkey    | d73a3bcda456f6113c2bd35bfe8776fe | 2025-11-26 20:44:35.015846
          22 |           6 | sales       | Table       | orders           | Index          | orders_pkey         | 0cecf5d617aede160696106c428d106a | 2025-11-26 20:44:35.015865
          23 |           6 | sales       | Table       | payments         | Index          | payments_pkey       | a66915faddc931836856b3749c75325b | 2025-11-26 20:44:35.015872
          24 |           6 | hr          | Table       | employees        | Index          | employees_pkey      | 93c3ea9faa198edd0fb3f21eeaadb84a | 2025-11-26 20:44:35.015888
(24 rows)

 select metadata_id,snapshot_id,schema_name,object_type,object_type_name,object_subtype,object_subtype_name,object_subtype_details,object_md5,processed_time from pdcd_schema.md5_metadata_staging_functions;
 metadata_id | snapshot_id | schema_name | object_type | object_type_name | object_subtype | object_subtype_name |                                                        object_subtype_details                                                        |            object_md5            |       processed_time
-------------+-------------+-------------+-------------+------------------+----------------+---------------------+--------------------------------------------------------------------------------------------------------------------------------------+----------------------------------+----------------------------
           1 |           6 | sales       | View        | v_orders_summary |                |                     | view_type:VIEW,view_definition: SELECT v_orders_master.customer_name,                                                               +| 00f9b1830b32611113eb4bd8bf039d04 | 2025-11-26 20:44:35.667879
             |             |             |             |                  |                |                     |     count(v_orders_master.order_id) AS total_orders                                                                                 +|                                  |
             |             |             |             |                  |                |                     |    FROM sales.v_orders_master                                                                                                       +|                                  |
             |             |             |             |                  |                |                     |   GROUP BY v_orders_master.customer_name;,base_tables:,view_owner:test_user,dependent_objects:Dependent views: sales.v_orders_master |                                  |
           2 |           6 | sales       | View        | v_orders_master  |                |                     | view_type:VIEW,view_definition: SELECT orders.order_id,                                                                             +| 8ade1ed7191ba1ff31a0d75a16c67096 | 2025-11-26 20:44:35.675145
             |             |             |             |                  |                |                     |     orders.customer_name                                                                                                            +|                                  |
             |             |             |             |                  |                |                     |    FROM sales.orders;,base_tables:sales.orders,view_owner:test_user,dependent_objects:                                               |                                  |
(2 rows)

--* Test Run 5 — Final Mixed Changes

```sql
ALTER VIEW sales.v_orders_summary RENAME TO v_orders_summary_change;
DROP VIEW sales.v_orders_master cascade;
CREATE VIEW sales.v_orders_master AS
SELECT order_id, customer_name, amount
FROM sales.orders;
```
    SELECT * FROM pdcd_schema.load_snapshot_tbl();
    SELECT * FROM pdcd_schema.compare_load_md5_metadata_tbl(ARRAY['sales','hr']);
    SELECT * from pdcd_schema.compare_load_md5_metadata_functions(ARRAY['sales','hr']);
    TRUNCATE TABLE pdcd_schema.md5_metadata_staging_tbl RESTART IDENTITY CASCADE;
    TRUNCATE TABLE pdcd_schema.md5_metadata_staging_functions RESTART IDENTITY CASCADE;
    SELECT * FROM pdcd_schema.load_md5_metadata_staging_tbl(ARRAY['sales','hr']);
    SELECT * from pdcd_schema.load_md5_metadata_staging_functions(ARRAY['sales','hr']);

select metadata_id,snapshot_id,schema_name,object_type,object_type_name,object_subtype,object_subtype_name,object_md5,processed_time,change_type FROM pdcd_schema.md5_metadata_tbl;
          51 |           7 | sales       | Table       | v_orders_master  | Column         | amount              | 9786c5f70226a57b52ffc90e55867d57 | 2025-11-26 20:48:08.458401 | ADDED
          52 |           7 | sales       | Table       | v_orders_summary | Column         | customer_name       | 7b6d79c39d7a5fec5b6f1f60762106d7 | 2025-11-26 20:48:08.458458 | DELETED
          53 |           7 | sales       | Table       | v_orders_summary | Column         | total_orders        | 281220c8e13d3604037c4cb48f84b433 | 2025-11-26 20:48:08.458472 | DELETED
          54 |           7 | sales       | View        | v_orders_master  |                |                     | 1e6b3547c6b4cb323d9f1e21d4ecec0f | 2025-11-26 20:48:09.363434 | MODIFIED
          55 |           7 | sales       | View        | v_orders_summary |                |                     | 00f9b1830b32611113eb4bd8bf039d04 | 2025-11-26 20:48:09.363888 | DELETED
(55 rows)

select metadata_id,snapshot_id,schema_name,object_type,object_type_name,object_subtype,object_subtype_name,object_md5,processed_time from pdcd_schema.md5_metadata_staging_tbl;
 metadata_id | snapshot_id | schema_name | object_type | object_type_name | object_subtype | object_subtype_name |            object_md5            |       processed_time
-------------+-------------+-------------+-------------+------------------+----------------+---------------------+----------------------------------+----------------------------
           1 |           7 | sales       | Table       | payments         | Column         | payment_id          | b9ef97ed1b56015241404938431b7501 | 2025-11-26 20:48:09.850399
           2 |           7 | sales       | Table       | v_orders_master  | Column         | order_id            | 3d8aeb78b1453edab2ecfd64244d4bbf | 2025-11-26 20:48:09.856885
           3 |           7 | hr          | Table       | employees        | Column         | emp_id              | 4ce3c147aa23959741647e8e8b674144 | 2025-11-26 20:48:09.85691
           4 |           7 | hr          | Table       | departments      | Column         | location            | eb892ea3bb6ebc1480b7b61c5fffb6db | 2025-11-26 20:48:09.856918
           5 |           7 | sales       | Table       | orders           | Column         | amount              | 9786c5f70226a57b52ffc90e55867d57 | 2025-11-26 20:48:09.856926
           6 |           7 | hr          | Table       | employees        | Column         | emp_name            | b537a75e4c2744b85e478bf937370ba9 | 2025-11-26 20:48:09.856931
           7 |           7 | sales       | Table       | payments         | Column         | payment_mode        | f37e89341863082d8eb1a9eaeb4eafce | 2025-11-26 20:48:09.856937
           8 |           7 | sales       | Table       | orders           | Column         | order_id            | b4a0f02b4504d1bb6a8914b4492d5605 | 2025-11-26 20:48:09.856943
           9 |           7 | sales       | Table       | v_orders_master  | Column         | customer_name       | b537a75e4c2744b85e478bf937370ba9 | 2025-11-26 20:48:09.856949
          10 |           7 | hr          | Table       | departments      | Column         | dept_name           | b537a75e4c2744b85e478bf937370ba9 | 2025-11-26 20:48:09.856955
          11 |           7 | sales       | Table       | payments         | Column         | order_id            | c1bd38510071b86246e457243c970262 | 2025-11-26 20:48:09.856961
          12 |           7 | sales       | Table       | orders           | Column         | customer_name       | b537a75e4c2744b85e478bf937370ba9 | 2025-11-26 20:48:09.856966
          13 |           7 | sales       | Table       | v_orders_master  | Column         | amount              | 9786c5f70226a57b52ffc90e55867d57 | 2025-11-26 20:48:09.856972
          14 |           7 | hr          | Table       | employees        | Column         | salary              | 9786c5f70226a57b52ffc90e55867d57 | 2025-11-26 20:48:09.856979
          15 |           7 | hr          | Table       | departments      | Column         | dept_id             | 94f0a420477141d95681142f0d0a58a7 | 2025-11-26 20:48:09.856985
          16 |           7 | sales       | Table       | orders           | Constraint     | orders_pkey         | a1a2e253f7d5944d5aa77c169bf3395d | 2025-11-26 20:48:09.859684
          17 |           7 | sales       | Table       | payments         | Constraint     | payments_pkey       | 1cb904dee1b3a8d68462afbe0e742561 | 2025-11-26 20:48:09.859705
          18 |           7 | hr          | Table       | departments      | Constraint     | departments_pkey    | 9e71d83424d14d7af950aa5eab35d0a1 | 2025-11-26 20:48:09.859712
          19 |           7 | hr          | Table       | employees        | Constraint     | employees_pkey      | 9e9d7894742421811810fa3c39c5489e | 2025-11-26 20:48:09.859718
          20 |           7 | hr          | Table       | departments      | Index          | departments_pkey    | d73a3bcda456f6113c2bd35bfe8776fe | 2025-11-26 20:48:09.862894
          21 |           7 | sales       | Table       | orders           | Index          | orders_pkey         | 0cecf5d617aede160696106c428d106a | 2025-11-26 20:48:09.862916
          22 |           7 | sales       | Table       | payments         | Index          | payments_pkey       | a66915faddc931836856b3749c75325b | 2025-11-26 20:48:09.862923
          23 |           7 | hr          | Table       | employees        | Index          | employees_pkey      | 93c3ea9faa198edd0fb3f21eeaadb84a | 2025-11-26 20:48:09.862929
(23 rows)

select metadata_id,snapshot_id,schema_name,object_type,object_type_name,object_subtype,object_subtype_name,object_md5,processed_time from pdcd_schema.md5_metadata_staging_functions;
 metadata_id | snapshot_id | schema_name | object_type | object_type_name | object_subtype | object_subtype_name |                                 object_subtype_details                                 |            object_md5            |       processed_time
-------------+-------------+-------------+-------------+------------------+----------------+---------------------+----------------------------------------------------------------------------------------+----------------------------------+----------------------------
           1 |           7 | sales       | View        | v_orders_master  |                |                     | view_type:VIEW,view_definition: SELECT orders.order_id,                               +| 1e6b3547c6b4cb323d9f1e21d4ecec0f | 2025-11-26 20:54:48.028999
             |             |             |             |                  |                |                     |     orders.customer_name,                                                             +|                                  |
             |             |             |             |                  |                |                     |     orders.amount                                                                     +|                                  |
             |             |             |             |                  |                |                     |    FROM sales.orders;,base_tables:sales.orders,view_owner:test_user,dependent_objects: |                                  |
(1 row)

-- for rename check
ALTER VIEW sales.v_orders_master RENAME TO v_orders_master_renamed; 

    SELECT * FROM pdcd_schema.load_snapshot_tbl();
    SELECT * FROM pdcd_schema.compare_load_md5_metadata_tbl(ARRAY['sales','hr']);
    SELECT * from pdcd_schema.compare_load_md5_metadata_functions(ARRAY['sales','hr']);
    TRUNCATE TABLE pdcd_schema.md5_metadata_staging_tbl RESTART IDENTITY CASCADE;
    TRUNCATE TABLE pdcd_schema.md5_metadata_staging_functions RESTART IDENTITY CASCADE;
    SELECT * FROM pdcd_schema.load_md5_metadata_staging_tbl(ARRAY['sales','hr']);
    SELECT * from pdcd_schema.load_md5_metadata_staging_functions(ARRAY['sales','hr']);

select metadata_id,snapshot_id,schema_name,object_type,object_type_name,object_subtype,object_subtype_name,object_md5,processed_time,change_type FROM pdcd_schema.md5_metadata_tbl;
          56 |           8 | sales       | Table       | v_orders_master_renamed | Column         | order_id            | 3d8aeb78b1453edab2ecfd64244d4bbf | 2025-11-26 20:57:08.871582 | ADDED
          57 |           8 | sales       | Table       | v_orders_master_renamed | Column         | customer_name       | b537a75e4c2744b85e478bf937370ba9 | 2025-11-26 20:57:08.871621 | ADDED
          58 |           8 | sales       | Table       | v_orders_master_renamed | Column         | amount              | 9786c5f70226a57b52ffc90e55867d57 | 2025-11-26 20:57:08.871627 | ADDED
          59 |           8 | sales       | Table       | v_orders_master         | Column         | order_id            | 3d8aeb78b1453edab2ecfd64244d4bbf | 2025-11-26 20:57:08.871665 | DELETED
          60 |           8 | sales       | Table       | v_orders_master         | Column         | customer_name       | b537a75e4c2744b85e478bf937370ba9 | 2025-11-26 20:57:08.871674 | DELETED
          61 |           8 | sales       | Table       | v_orders_master         | Column         | amount              | 9786c5f70226a57b52ffc90e55867d57 | 2025-11-26 20:57:08.871693 | DELETED
          62 |           8 | sales       | View        | v_orders_master_renamed |                |                     | 1e6b3547c6b4cb323d9f1e21d4ecec0f | 2025-11-26 20:57:09.945631 | RENAMED
(62 rows)

select metadata_id,snapshot_id,schema_name,object_type,object_type_name,object_subtype,object_subtype_name,object_md5,processed_time from pdcd_schema.md5_metadata_staging_tbl;
 metadata_id | snapshot_id | schema_name | object_type |    object_type_name     | object_subtype | object_subtype_name |            object_md5            |       processed_time
-------------+-------------+-------------+-------------+-------------------------+----------------+---------------------+----------------------------------+----------------------------
           1 |           8 | sales       | Table       | v_orders_master_renamed | Column         | customer_name       | b537a75e4c2744b85e478bf937370ba9 | 2025-11-26 20:57:09.999757
           2 |           8 | sales       | Table       | payments                | Column         | payment_id          | b9ef97ed1b56015241404938431b7501 | 2025-11-26 20:57:10.006161
           3 |           8 | hr          | Table       | employees               | Column         | emp_id              | 4ce3c147aa23959741647e8e8b674144 | 2025-11-26 20:57:10.006168
           4 |           8 | hr          | Table       | departments             | Column         | location            | eb892ea3bb6ebc1480b7b61c5fffb6db | 2025-11-26 20:57:10.006172
           5 |           8 | sales       | Table       | orders                  | Column         | amount              | 9786c5f70226a57b52ffc90e55867d57 | 2025-11-26 20:57:10.006175
           6 |           8 | hr          | Table       | employees               | Column         | emp_name            | b537a75e4c2744b85e478bf937370ba9 | 2025-11-26 20:57:10.006177
           7 |           8 | sales       | Table       | payments                | Column         | payment_mode        | f37e89341863082d8eb1a9eaeb4eafce | 2025-11-26 20:57:10.00618
           8 |           8 | sales       | Table       | orders                  | Column         | order_id            | b4a0f02b4504d1bb6a8914b4492d5605 | 2025-11-26 20:57:10.006182
           9 |           8 | hr          | Table       | departments             | Column         | dept_name           | b537a75e4c2744b85e478bf937370ba9 | 2025-11-26 20:57:10.006185
          10 |           8 | sales       | Table       | payments                | Column         | order_id            | c1bd38510071b86246e457243c970262 | 2025-11-26 20:57:10.006188
          11 |           8 | sales       | Table       | orders                  | Column         | customer_name       | b537a75e4c2744b85e478bf937370ba9 | 2025-11-26 20:57:10.006191
          12 |           8 | sales       | Table       | v_orders_master_renamed | Column         | order_id            | 3d8aeb78b1453edab2ecfd64244d4bbf | 2025-11-26 20:57:10.006194
          13 |           8 | sales       | Table       | v_orders_master_renamed | Column         | amount              | 9786c5f70226a57b52ffc90e55867d57 | 2025-11-26 20:57:10.006197
          14 |           8 | hr          | Table       | employees               | Column         | salary              | 9786c5f70226a57b52ffc90e55867d57 | 2025-11-26 20:57:10.006199
          15 |           8 | hr          | Table       | departments             | Column         | dept_id             | 94f0a420477141d95681142f0d0a58a7 | 2025-11-26 20:57:10.006202
          16 |           8 | sales       | Table       | orders                  | Constraint     | orders_pkey         | a1a2e253f7d5944d5aa77c169bf3395d | 2025-11-26 20:57:10.007621
          17 |           8 | sales       | Table       | payments                | Constraint     | payments_pkey       | 1cb904dee1b3a8d68462afbe0e742561 | 2025-11-26 20:57:10.007627
          18 |           8 | hr          | Table       | departments             | Constraint     | departments_pkey    | 9e71d83424d14d7af950aa5eab35d0a1 | 2025-11-26 20:57:10.00763
          19 |           8 | hr          | Table       | employees               | Constraint     | employees_pkey      | 9e9d7894742421811810fa3c39c5489e | 2025-11-26 20:57:10.007633
          20 |           8 | hr          | Table       | departments             | Index          | departments_pkey    | d73a3bcda456f6113c2bd35bfe8776fe | 2025-11-26 20:57:10.009173
          21 |           8 | sales       | Table       | orders                  | Index          | orders_pkey         | 0cecf5d617aede160696106c428d106a | 2025-11-26 20:57:10.009179
          22 |           8 | sales       | Table       | payments                | Index          | payments_pkey       | a66915faddc931836856b3749c75325b | 2025-11-26 20:57:10.009182
          23 |           8 | hr          | Table       | employees               | Index          | employees_pkey      | 93c3ea9faa198edd0fb3f21eeaadb84a | 2025-11-26 20:57:10.009185
(23 rows)

select metadata_id,snapshot_id,schema_name,object_type,object_type_name,object_subtype,object_subtype_name,object_subtype_details,object_md5,processed_time from pdcd_schema.md5_metadata_staging_functions;
 metadata_id | snapshot_id | schema_name | object_type |    object_type_name     | object_subtype | object_subtype_name |                                 object_subtype_details                                 |            object_md5            |       processed_time
-------------+-------------+-------------+-------------+-------------------------+----------------+---------------------+----------------------------------------------------------------------------------------+----------------------------------+----------------------------
           1 |           8 | sales       | View        | v_orders_master_renamed |                |                     | view_type:VIEW,view_definition: SELECT orders.order_id,                               +| 1e6b3547c6b4cb323d9f1e21d4ecec0f | 2025-11-26 20:57:10.271412
             |             |             |             |                         |                |                     |     orders.customer_name,                                                             +|                                  |
             |             |             |             |                         |                |                     |     orders.amount                                                                     +|                                  |
             |             |             |             |                         |                |                     |    FROM sales.orders;,base_tables:sales.orders,view_owner:test_user,dependent_objects: |                                  |
(1 row)


--* Test Run 6 — INSTEAD OF Trigger on View

### Create Trigger Function

```sql
CREATE OR REPLACE FUNCTION sales.trg_insert_orders()
RETURNS trigger AS $$
BEGIN
    INSERT INTO sales.orders(order_id, customer_name, amount)
    VALUES (NEW.order_id, NEW.customer_name, NEW.amount);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
```

### Attach Trigger

```sql
CREATE TRIGGER trg_orders_insert
INSTEAD OF INSERT ON sales.v_orders_master_renamed
FOR EACH ROW EXECUTE FUNCTION sales.trg_insert_orders();
```
    SELECT * FROM pdcd_schema.load_snapshot_tbl();
    SELECT * FROM pdcd_schema.compare_load_md5_metadata_tbl(ARRAY['sales','hr']);
    SELECT * from pdcd_schema.compare_load_md5_metadata_functions(ARRAY['sales','hr']);
    TRUNCATE TABLE pdcd_schema.md5_metadata_staging_tbl RESTART IDENTITY CASCADE;
    TRUNCATE TABLE pdcd_schema.md5_metadata_staging_functions RESTART IDENTITY CASCADE;
    SELECT * FROM pdcd_schema.load_md5_metadata_staging_tbl(ARRAY['sales','hr']);
    SELECT * from pdcd_schema.load_md5_metadata_staging_functions(ARRAY['sales','hr']);

select metadata_id,snapshot_id,schema_name,object_type,object_type_name,object_subtype,object_subtype_name,object_md5,processed_time,change_type FROM pdcd_schema.md5_metadata_tbl;
          64 |           9 | sales       | Function    | trg_insert_orders       |                |                     | 886d4e3202cebaa208d02dc87595883e | 2025-11-26 21:01:14.505879 | ADDED
          65 |          10 | sales       | View        | v_orders_master_renamed | Trigger        | trg_orders_insert   | 80e497b43ab139c8c4392ac54944a64c | 2025-11-26 21:30:44.165419 | ADDED
(65 rows)

select metadata_id,snapshot_id,schema_name,object_type,object_type_name,object_subtype,object_subtype_name,object_md5,processed_time from pdcd_schema.md5_metadata_staging_tbl;
 metadata_id | snapshot_id | schema_name | object_type |    object_type_name     | object_subtype | object_subtype_name |            object_md5            |       processed_time
-------------+-------------+-------------+-------------+-------------------------+----------------+---------------------+----------------------------------+----------------------------
           1 |          10 | sales       | Table       | v_orders_master_renamed | Column         | customer_name       | b537a75e4c2744b85e478bf937370ba9 | 2025-11-26 21:31:08.122249
           2 |          10 | sales       | Table       | payments                | Column         | payment_id          | b9ef97ed1b56015241404938431b7501 | 2025-11-26 21:31:08.129813
           3 |          10 | hr          | Table       | employees               | Column         | emp_id              | 4ce3c147aa23959741647e8e8b674144 | 2025-11-26 21:31:08.129831
           4 |          10 | hr          | Table       | departments             | Column         | location            | eb892ea3bb6ebc1480b7b61c5fffb6db | 2025-11-26 21:31:08.129838
           5 |          10 | sales       | Table       | orders                  | Column         | amount              | 9786c5f70226a57b52ffc90e55867d57 | 2025-11-26 21:31:08.129844
           6 |          10 | hr          | Table       | employees               | Column         | emp_name            | b537a75e4c2744b85e478bf937370ba9 | 2025-11-26 21:31:08.129863
           7 |          10 | sales       | Table       | payments                | Column         | payment_mode        | f37e89341863082d8eb1a9eaeb4eafce | 2025-11-26 21:31:08.129869
           8 |          10 | sales       | Table       | orders                  | Column         | order_id            | b4a0f02b4504d1bb6a8914b4492d5605 | 2025-11-26 21:31:08.129876
           9 |          10 | hr          | Table       | departments             | Column         | dept_name           | b537a75e4c2744b85e478bf937370ba9 | 2025-11-26 21:31:08.129881
          10 |          10 | sales       | Table       | payments                | Column         | order_id            | c1bd38510071b86246e457243c970262 | 2025-11-26 21:31:08.129887
          11 |          10 | sales       | Table       | orders                  | Column         | customer_name       | b537a75e4c2744b85e478bf937370ba9 | 2025-11-26 21:31:08.129903
          12 |          10 | sales       | Table       | v_orders_master_renamed | Column         | order_id            | 3d8aeb78b1453edab2ecfd64244d4bbf | 2025-11-26 21:31:08.12991
          13 |          10 | sales       | Table       | v_orders_master_renamed | Column         | amount              | 9786c5f70226a57b52ffc90e55867d57 | 2025-11-26 21:31:08.129915
          14 |          10 | hr          | Table       | employees               | Column         | salary              | 9786c5f70226a57b52ffc90e55867d57 | 2025-11-26 21:31:08.12992
          15 |          10 | hr          | Table       | departments             | Column         | dept_id             | 94f0a420477141d95681142f0d0a58a7 | 2025-11-26 21:31:08.129926
          16 |          10 | sales       | Table       | orders                  | Constraint     | orders_pkey         | a1a2e253f7d5944d5aa77c169bf3395d | 2025-11-26 21:31:08.132841
          17 |          10 | sales       | Table       | payments                | Constraint     | payments_pkey       | 1cb904dee1b3a8d68462afbe0e742561 | 2025-11-26 21:31:08.132863
          18 |          10 | hr          | Table       | departments             | Constraint     | departments_pkey    | 9e71d83424d14d7af950aa5eab35d0a1 | 2025-11-26 21:31:08.132869
          19 |          10 | hr          | Table       | employees               | Constraint     | employees_pkey      | 9e9d7894742421811810fa3c39c5489e | 2025-11-26 21:31:08.132874
          20 |          10 | hr          | Table       | departments             | Index          | departments_pkey    | d73a3bcda456f6113c2bd35bfe8776fe | 2025-11-26 21:31:08.136189
          21 |          10 | sales       | Table       | orders                  | Index          | orders_pkey         | 0cecf5d617aede160696106c428d106a | 2025-11-26 21:31:08.13623
          22 |          10 | sales       | Table       | payments                | Index          | payments_pkey       | a66915faddc931836856b3749c75325b | 2025-11-26 21:31:08.136237
          23 |          10 | hr          | Table       | employees               | Index          | employees_pkey      | 93c3ea9faa198edd0fb3f21eeaadb84a | 2025-11-26 21:31:08.136243
          24 |          10 | sales       | View        | v_orders_master_renamed | Trigger        | trg_orders_insert   | 80e497b43ab139c8c4392ac54944a64c | 2025-11-26 21:31:08.142145
(24 rows)

select metadata_id,snapshot_id,schema_name,object_type,object_type_name,object_subtype,object_subtype_name,object_md5,processed_time from pdcd_schema.md5_metadata_staging_functions;
 metadata_id | snapshot_id | schema_name | object_type |    object_type_name     | object_subtype | object_subtype_name |            object_md5            |       processed_time
-------------+-------------+-------------+-------------+-------------------------+----------------+---------------------+----------------------------------+----------------------------
           1 |          10 | sales       | Function    | trg_insert_orders       |                |                     | 886d4e3202cebaa208d02dc87595883e | 2025-11-26 21:31:09.467654
           2 |          10 | sales       | View        | v_orders_master_renamed |                |                     | c6e6de91ce0e1b084f44044bf97b1d29 | 2025-11-26 21:31:09.489948
(2 rows)
---

--* Test Run 7 — Dependent View Change

```sql
CREATE OR REPLACE VIEW sales.v_orders_high_value AS
SELECT order_id, customer_name
FROM sales.v_orders_master_renamed
WHERE amount > 5000;
```

    SELECT * FROM pdcd_schema.load_snapshot_tbl();
    SELECT * FROM pdcd_schema.compare_load_md5_metadata_tbl(ARRAY['sales','hr']);
    SELECT * from pdcd_schema.compare_load_md5_metadata_functions(ARRAY['sales','hr']);
    TRUNCATE TABLE pdcd_schema.md5_metadata_staging_tbl RESTART IDENTITY CASCADE;
    TRUNCATE TABLE pdcd_schema.md5_metadata_staging_functions RESTART IDENTITY CASCADE;
    SELECT * FROM pdcd_schema.load_md5_metadata_staging_tbl(ARRAY['sales','hr']);
    SELECT * from pdcd_schema.load_md5_metadata_staging_functions(ARRAY['sales','hr']);

select metadata_id,snapshot_id,schema_name,object_type,object_type_name,object_subtype,object_subtype_name,object_md5,processed_time,change_type FROM pdcd_schema.md5_metadata_tbl;
          66 |          11 | sales       | Table       | v_orders_high_value     | Column         | order_id            | 3d8aeb78b1453edab2ecfd64244d4bbf | 2025-11-26 21:34:04.289088 | ADDED
          67 |          11 | sales       | Table       | v_orders_high_value     | Column         | customer_name       | b537a75e4c2744b85e478bf937370ba9 | 2025-11-26 21:34:04.289134 | ADDED
          68 |          11 | sales       | View        | v_orders_high_value     |                |                     | f368e7c604ba704c059b5dbcaf3e2e7f | 2025-11-26 21:34:05.619614 | ADDED
(68 rows)

select metadata_id,snapshot_id,schema_name,object_type,object_type_name,object_subtype,object_subtype_name,object_md5,processed_time from pdcd_schema.md5_metadata_staging_tbl;
 metadata_id | snapshot_id | schema_name | object_type |    object_type_name     | object_subtype | object_subtype_name |            object_md5            |       processed_time
-------------+-------------+-------------+-------------+-------------------------+----------------+---------------------+----------------------------------+----------------------------
           1 |          11 | sales       | Table       | v_orders_high_value     | Column         | customer_name       | b537a75e4c2744b85e478bf937370ba9 | 2025-11-26 21:34:06.24999
           2 |          11 | sales       | Table       | v_orders_master_renamed | Column         | customer_name       | b537a75e4c2744b85e478bf937370ba9 | 2025-11-26 21:34:06.254831
           3 |          11 | sales       | Table       | payments                | Column         | payment_id          | b9ef97ed1b56015241404938431b7501 | 2025-11-26 21:34:06.254857
           4 |          11 | hr          | Table       | employees               | Column         | emp_id              | 4ce3c147aa23959741647e8e8b674144 | 2025-11-26 21:34:06.254865
           5 |          11 | hr          | Table       | departments             | Column         | location            | eb892ea3bb6ebc1480b7b61c5fffb6db | 2025-11-26 21:34:06.254873
           6 |          11 | sales       | Table       | orders                  | Column         | amount              | 9786c5f70226a57b52ffc90e55867d57 | 2025-11-26 21:34:06.254879
           7 |          11 | hr          | Table       | employees               | Column         | emp_name            | b537a75e4c2744b85e478bf937370ba9 | 2025-11-26 21:34:06.254884
           8 |          11 | sales       | Table       | payments                | Column         | payment_mode        | f37e89341863082d8eb1a9eaeb4eafce | 2025-11-26 21:34:06.25489
           9 |          11 | sales       | Table       | v_orders_high_value     | Column         | order_id            | 3d8aeb78b1453edab2ecfd64244d4bbf | 2025-11-26 21:34:06.254896
          10 |          11 | sales       | Table       | orders                  | Column         | order_id            | b4a0f02b4504d1bb6a8914b4492d5605 | 2025-11-26 21:34:06.254903
          11 |          11 | hr          | Table       | departments             | Column         | dept_name           | b537a75e4c2744b85e478bf937370ba9 | 2025-11-26 21:34:06.254921
          12 |          11 | sales       | Table       | payments                | Column         | order_id            | c1bd38510071b86246e457243c970262 | 2025-11-26 21:34:06.254927
          13 |          11 | sales       | Table       | orders                  | Column         | customer_name       | b537a75e4c2744b85e478bf937370ba9 | 2025-11-26 21:34:06.254932
          14 |          11 | sales       | Table       | v_orders_master_renamed | Column         | order_id            | 3d8aeb78b1453edab2ecfd64244d4bbf | 2025-11-26 21:34:06.25494
          15 |          11 | sales       | Table       | v_orders_master_renamed | Column         | amount              | 9786c5f70226a57b52ffc90e55867d57 | 2025-11-26 21:34:06.254948
          16 |          11 | hr          | Table       | employees               | Column         | salary              | 9786c5f70226a57b52ffc90e55867d57 | 2025-11-26 21:34:06.254954
          17 |          11 | hr          | Table       | departments             | Column         | dept_id             | 94f0a420477141d95681142f0d0a58a7 | 2025-11-26 21:34:06.25496
          18 |          11 | sales       | Table       | orders                  | Constraint     | orders_pkey         | a1a2e253f7d5944d5aa77c169bf3395d | 2025-11-26 21:34:06.258182
          19 |          11 | sales       | Table       | payments                | Constraint     | payments_pkey       | 1cb904dee1b3a8d68462afbe0e742561 | 2025-11-26 21:34:06.258207
          20 |          11 | hr          | Table       | departments             | Constraint     | departments_pkey    | 9e71d83424d14d7af950aa5eab35d0a1 | 2025-11-26 21:34:06.258214
          21 |          11 | hr          | Table       | employees               | Constraint     | employees_pkey      | 9e9d7894742421811810fa3c39c5489e | 2025-11-26 21:34:06.25822
          22 |          11 | hr          | Table       | departments             | Index          | departments_pkey    | d73a3bcda456f6113c2bd35bfe8776fe | 2025-11-26 21:34:06.262638
          23 |          11 | sales       | Table       | orders                  | Index          | orders_pkey         | 0cecf5d617aede160696106c428d106a | 2025-11-26 21:34:06.262655
          24 |          11 | sales       | Table       | payments                | Index          | payments_pkey       | a66915faddc931836856b3749c75325b | 2025-11-26 21:34:06.262659
          25 |          11 | hr          | Table       | employees               | Index          | employees_pkey      | 93c3ea9faa198edd0fb3f21eeaadb84a | 2025-11-26 21:34:06.262662
          26 |          11 | sales       | View        | v_orders_master_renamed | Trigger        | trg_orders_insert   | 80e497b43ab139c8c4392ac54944a64c | 2025-11-26 21:34:06.268704
(26 rows)

select metadata_id,snapshot_id,schema_name,object_type,object_type_name,object_subtype,object_subtype_name,object_md5,processed_time from pdcd_schema.md5_metadata_staging_functions;
 metadata_id | snapshot_id | schema_name | object_type |    object_type_name     | object_subtype | object_subtype_name |            object_md5            |       processed_time
-------------+-------------+-------------+-------------+-------------------------+----------------+---------------------+----------------------------------+----------------------------
           1 |          11 | sales       | Function    | trg_insert_orders       |                |                     | 886d4e3202cebaa208d02dc87595883e | 2025-11-26 21:34:06.999814
           2 |          11 | sales       | View        | v_orders_high_value     |                |                     | f368e7c604ba704c059b5dbcaf3e2e7f | 2025-11-26 21:34:07.018584
           3 |          11 | sales       | View        | v_orders_master_renamed |                |                     | c6e6de91ce0e1b084f44044bf97b1d29 | 2025-11-26 21:34:07.018613
(3 rows)


--* Test Run 8 — Drop & Recreate Trigger

```sql
DROP TRIGGER trg_orders_insert ON sales.v_orders_master_renamed;

CREATE TRIGGER trg_orders_insert
INSTEAD OF INSERT ON sales.v_orders_master_renamed
FOR EACH ROW EXECUTE FUNCTION sales.trg_insert_orders();
```

    SELECT * FROM pdcd_schema.load_snapshot_tbl();
    SELECT * FROM pdcd_schema.compare_load_md5_metadata_tbl(ARRAY['sales','hr']);
    SELECT * from pdcd_schema.compare_load_md5_metadata_functions(ARRAY['sales','hr']);
    TRUNCATE TABLE pdcd_schema.md5_metadata_staging_tbl RESTART IDENTITY CASCADE;
    TRUNCATE TABLE pdcd_schema.md5_metadata_staging_functions RESTART IDENTITY CASCADE;
    SELECT * FROM pdcd_schema.load_md5_metadata_staging_tbl(ARRAY['sales','hr']);
    SELECT * from pdcd_schema.load_md5_metadata_staging_functions(ARRAY['sales','hr']);

select metadata_id,snapshot_id,schema_name,object_type,object_type_name,object_subtype,object_subtype_name,object_md5,processed_time,change_type FROM pdcd_schema.md5_metadata_tbl;
          66 |          11 | sales       | Table       | v_orders_high_value     | Column         | order_id            | 3d8aeb78b1453edab2ecfd64244d4bbf | 2025-11-26 21:34:04.289088 | ADDED
          67 |          11 | sales       | Table       | v_orders_high_value     | Column         | customer_name       | b537a75e4c2744b85e478bf937370ba9 | 2025-11-26 21:34:04.289134 | ADDED
          68 |          11 | sales       | View        | v_orders_high_value     |                |                     | f368e7c604ba704c059b5dbcaf3e2e7f | 2025-11-26 21:34:05.619614 | ADDED
(68 rows)
-- unchanged after drop and recreate of trigger
