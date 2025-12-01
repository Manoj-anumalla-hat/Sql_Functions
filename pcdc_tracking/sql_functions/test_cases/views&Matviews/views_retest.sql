---- 

CREATE SCHEMA sales;

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

-------
CREATE SCHEMA hr;

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

--- base view
CREATE VIEW sales.v_orders_base AS
SELECT order_id, customer_name, amount
FROM sales.orders;

-- dependent view
CREATE VIEW sales.v_orders_report AS
SELECT order_id, customer_name
FROM sales.v_orders_base
WHERE amount > 1000;


-- First Run
SELECT * FROM pdcd_schema.load_snapshot_tbl();
SELECT * FROM pdcd_schema.load_md5_metadata_tbl(ARRAY['sales','hr']);
SELECT * FROM pdcd_schema.load_md5_metadata_staging_tbl(ARRAY['sales','hr']);
SELECT * from pdcd_schema.load_md5_metadata_staging_functions(ARRAY['sales','hr']);

-- md5_metadata_tbl results
select metadata_id,snapshot_id,schema_name,object_type,object_type_name,object_subtype,object_subtype_name,object_md5,processed_time,change_type FROM pdcd_schema.md5_metadata_tbl;
metadata_id | snapshot_id | schema_name | object_type | object_type_name | object_subtype | object_subtype_name |            object_md5            |       processed_time       | change_type
-------------+-------------+-------------+-------------+------------------+----------------+---------------------+----------------------------------+----------------------------+-------------
           1 |           1 | hr          | Table       | departments      | Column         | dept_id             | 94f0a420477141d95681142f0d0a58a7 | 2025-11-25 22:31:54.487446 | ADDED
           2 |           1 | hr          | Table       | departments      | Column         | dept_name           | b537a75e4c2744b85e478bf937370ba9 | 2025-11-25 22:31:54.490399 | ADDED
           3 |           1 | hr          | Table       | departments      | Column         | location            | eb892ea3bb6ebc1480b7b61c5fffb6db | 2025-11-25 22:31:54.490415 | ADDED
           4 |           1 | hr          | Table       | employees        | Column         | emp_id              | 4ce3c147aa23959741647e8e8b674144 | 2025-11-25 22:31:54.490422 | ADDED
           5 |           1 | hr          | Table       | employees        | Column         | emp_name            | b537a75e4c2744b85e478bf937370ba9 | 2025-11-25 22:31:54.490429 | ADDED
           6 |           1 | hr          | Table       | employees        | Column         | salary              | 9786c5f70226a57b52ffc90e55867d57 | 2025-11-25 22:31:54.490435 | ADDED
           7 |           1 | sales       | Table       | orders           | Column         | order_id            | b4a0f02b4504d1bb6a8914b4492d5605 | 2025-11-25 22:31:54.49044  | ADDED
           8 |           1 | sales       | Table       | orders           | Column         | customer_name       | b537a75e4c2744b85e478bf937370ba9 | 2025-11-25 22:31:54.490446 | ADDED
           9 |           1 | sales       | Table       | orders           | Column         | amount              | 9786c5f70226a57b52ffc90e55867d57 | 2025-11-25 22:31:54.490451 | ADDED
          10 |           1 | sales       | Table       | payments         | Column         | payment_id          | b9ef97ed1b56015241404938431b7501 | 2025-11-25 22:31:54.490457 | ADDED
          11 |           1 | sales       | Table       | payments         | Column         | order_id            | c1bd38510071b86246e457243c970262 | 2025-11-25 22:31:54.490463 | ADDED
          12 |           1 | sales       | Table       | payments         | Column         | payment_mode        | f37e89341863082d8eb1a9eaeb4eafce | 2025-11-25 22:31:54.490468 | ADDED
          13 |           1 | sales       | Table       | v_orders_base    | Column         | order_id            | 3d8aeb78b1453edab2ecfd64244d4bbf | 2025-11-25 22:31:54.490473 | ADDED
          14 |           1 | sales       | Table       | v_orders_base    | Column         | customer_name       | b537a75e4c2744b85e478bf937370ba9 | 2025-11-25 22:31:54.490478 | ADDED
          15 |           1 | sales       | Table       | v_orders_base    | Column         | amount              | 9786c5f70226a57b52ffc90e55867d57 | 2025-11-25 22:31:54.490484 | ADDED
          16 |           1 | sales       | Table       | v_orders_report  | Column         | order_id            | 3d8aeb78b1453edab2ecfd64244d4bbf | 2025-11-25 22:31:54.490489 | ADDED
          17 |           1 | sales       | Table       | v_orders_report  | Column         | customer_name       | b537a75e4c2744b85e478bf937370ba9 | 2025-11-25 22:31:54.490495 | ADDED
          18 |           1 | hr          | Table       | departments      | Constraint     | departments_pkey    | 9e71d83424d14d7af950aa5eab35d0a1 | 2025-11-25 22:31:54.493844 | ADDED
          19 |           1 | hr          | Table       | employees        | Constraint     | employees_pkey      | 9e9d7894742421811810fa3c39c5489e | 2025-11-25 22:31:54.493867 | ADDED
          20 |           1 | sales       | Table       | orders           | Constraint     | orders_pkey         | a1a2e253f7d5944d5aa77c169bf3395d | 2025-11-25 22:31:54.493873 | ADDED
          21 |           1 | sales       | Table       | payments         | Constraint     | payments_pkey       | 1cb904dee1b3a8d68462afbe0e742561 | 2025-11-25 22:31:54.493878 | ADDED
          22 |           1 | hr          | Table       | departments      | Index          | departments_pkey    | 2b27c7ee86642bc7e5ae5b193f5da326 | 2025-11-25 22:31:54.498014 | ADDED
          23 |           1 | hr          | Table       | employees        | Index          | employees_pkey      | 99a6170f23a2bc6877a066d2c85b831f | 2025-11-25 22:31:54.498043 | ADDED
          24 |           1 | sales       | Table       | orders           | Index          | orders_pkey         | 6239d506ccdee51d853851210313cb4b | 2025-11-25 22:31:54.49805  | ADDED
          25 |           1 | sales       | Table       | payments         | Index          | payments_pkey       | 4444f1a676edffaae5750703d4e53f4e | 2025-11-25 22:31:54.498056 | ADDED
          26 |           1 | sales       | View        | v_orders_base    |                |                     | 1e6b3547c6b4cb323d9f1e21d4ecec0f | 2025-11-25 22:31:54.517711 | ADDED
          27 |           1 | sales       | View        | v_orders_report  |                |                     | c09fcd23b41c82a278650e57f84d7dd3 | 2025-11-25 22:31:54.517741 | ADDED
(27 rows)

-- md5_metadata_staging_tbl results
select metadata_id,snapshot_id,schema_name,object_type,object_type_name,object_subtype,object_subtype_name,object_md5,processed_time FROM pdcd_schema.md5_metadata_staging_tbl;
 metadata_id | snapshot_id | schema_name | object_type | object_type_name | object_subtype | object_subtype_name |            object_md5            |       processed_time
-------------+-------------+-------------+-------------+------------------+----------------+---------------------+----------------------------------+----------------------------
           1 |           1 | sales       | Table       | v_orders_report  | Column         | order_id            | 3d8aeb78b1453edab2ecfd64244d4bbf | 2025-11-25 22:32:08.5279
           2 |           1 | sales       | Table       | v_orders_report  | Column         | customer_name       | b537a75e4c2744b85e478bf937370ba9 | 2025-11-25 22:32:08.532327
           3 |           1 | sales       | Table       | payments         | Column         | payment_id          | b9ef97ed1b56015241404938431b7501 | 2025-11-25 22:32:08.53235
           4 |           1 | hr          | Table       | employees        | Column         | emp_id              | 4ce3c147aa23959741647e8e8b674144 | 2025-11-25 22:32:08.53236
           5 |           1 | sales       | Table       | v_orders_base    | Column         | order_id            | 3d8aeb78b1453edab2ecfd64244d4bbf | 2025-11-25 22:32:08.53237
           6 |           1 | hr          | Table       | departments      | Column         | location            | eb892ea3bb6ebc1480b7b61c5fffb6db | 2025-11-25 22:32:08.53238
           7 |           1 | sales       | Table       | orders           | Column         | amount              | 9786c5f70226a57b52ffc90e55867d57 | 2025-11-25 22:32:08.532388
           8 |           1 | hr          | Table       | employees        | Column         | emp_name            | b537a75e4c2744b85e478bf937370ba9 | 2025-11-25 22:32:08.532395
           9 |           1 | sales       | Table       | payments         | Column         | payment_mode        | f37e89341863082d8eb1a9eaeb4eafce | 2025-11-25 22:32:08.532403
          10 |           1 | sales       | Table       | orders           | Column         | order_id            | b4a0f02b4504d1bb6a8914b4492d5605 | 2025-11-25 22:32:08.532412
          11 |           1 | sales       | Table       | v_orders_base    | Column         | amount              | 9786c5f70226a57b52ffc90e55867d57 | 2025-11-25 22:32:08.532422
          12 |           1 | hr          | Table       | departments      | Column         | dept_name           | b537a75e4c2744b85e478bf937370ba9 | 2025-11-25 22:32:08.532431
          13 |           1 | sales       | Table       | payments         | Column         | order_id            | c1bd38510071b86246e457243c970262 | 2025-11-25 22:32:08.53244
          14 |           1 | sales       | Table       | orders           | Column         | customer_name       | b537a75e4c2744b85e478bf937370ba9 | 2025-11-25 22:32:08.532446
          15 |           1 | sales       | Table       | v_orders_base    | Column         | customer_name       | b537a75e4c2744b85e478bf937370ba9 | 2025-11-25 22:32:08.532454
          16 |           1 | hr          | Table       | employees        | Column         | salary              | 9786c5f70226a57b52ffc90e55867d57 | 2025-11-25 22:32:08.532462
          17 |           1 | hr          | Table       | departments      | Column         | dept_id             | 94f0a420477141d95681142f0d0a58a7 | 2025-11-25 22:32:08.532484
          18 |           1 | sales       | Table       | orders           | Constraint     | orders_pkey         | a1a2e253f7d5944d5aa77c169bf3395d | 2025-11-25 22:32:08.535571
          19 |           1 | sales       | Table       | payments         | Constraint     | payments_pkey       | 1cb904dee1b3a8d68462afbe0e742561 | 2025-11-25 22:32:08.535589
          20 |           1 | hr          | Table       | departments      | Constraint     | departments_pkey    | 9e71d83424d14d7af950aa5eab35d0a1 | 2025-11-25 22:32:08.535595
          21 |           1 | hr          | Table       | employees        | Constraint     | employees_pkey      | 9e9d7894742421811810fa3c39c5489e | 2025-11-25 22:32:08.535602
          22 |           1 | hr          | Table       | departments      | Index          | departments_pkey    | 2b27c7ee86642bc7e5ae5b193f5da326 | 2025-11-25 22:32:08.539048
          23 |           1 | hr          | Table       | employees        | Index          | employees_pkey      | 99a6170f23a2bc6877a066d2c85b831f | 2025-11-25 22:32:08.539065
          24 |           1 | sales       | Table       | payments         | Index          | payments_pkey       | 4444f1a676edffaae5750703d4e53f4e | 2025-11-25 22:32:08.539071
          25 |           1 | sales       | Table       | orders           | Index          | orders_pkey         | 6239d506ccdee51d853851210313cb4b | 2025-11-25 22:32:08.539078
(25 rows)

-- md5_metadata_staging_functions results
select metadata_id,snapshot_id,schema_name,object_type,object_type_name,object_subtype,object_subtype_name,object_md5,processed_time FROM pdcd_schema.md5_metadata_staging_functions;
 metadata_id | snapshot_id | schema_name | object_type | object_type_name | object_subtype | object_subtype_name |            object_md5            |       processed_time
-------------+-------------+-------------+-------------+------------------+----------------+---------------------+----------------------------------+----------------------------
           1 |           1 | sales       | View        | v_orders_base    |                |                     | 1e6b3547c6b4cb323d9f1e21d4ecec0f | 2025-11-25 22:32:15.36668
           2 |           1 | sales       | View        | v_orders_report  |                |                     | c09fcd23b41c82a278650e57f84d7dd3 | 2025-11-25 22:32:15.370108
(2 rows)


-- TEST RUN 1 — Add & Modification

CREATE OR REPLACE VIEW sales.v_orders_base AS
SELECT order_id, customer_name, amount
FROM sales.orders
WHERE amount IS NOT NULL;

    SELECT * FROM pdcd_schema.load_snapshot_tbl();
  
    SELECT * from pdcd_schema.compare_load_md5_metadata_functions(ARRAY['sales','hr']);

    TRUNCATE TABLE pdcd_schema.md5_metadata_staging_functions RESTART IDENTITY CASCADE;

    SELECT * from pdcd_schema.load_md5_metadata_staging_functions(ARRAY['sales','hr']);

select metadata_id,snapshot_id,schema_name,object_type,object_type_name,object_subtype,object_subtype_name,object_md5,processed_time,change_type FROM pdcd_schema.md5_metadata_tbl;
         26 |           1 | sales       | View        | v_orders_base    |                |                     | 1e6b3547c6b4cb323d9f1e21d4ecec0f | 2025-11-25 22:31:54.517711 | ADDED
         27 |           1 | sales       | View        | v_orders_report  |                |                     | c09fcd23b41c82a278650e57f84d7dd3 | 2025-11-25 22:31:54.517741 | ADDED
         28 |           2 | sales       | View        | v_orders_base    |                |                     | 8ab5ff64b11f49649311e714ba77d48c | 2025-11-25 22:35:08.461134 | MODIFIED
(28 rows)

select metadata_id,snapshot_id,schema_name,object_type,object_type_name,object_subtype,object_subtype_name,object_md5,processed_time FROM pdcd_schema.md5_metadata_staging_functions;
 metadata_id | snapshot_id | schema_name | object_type | object_type_name | object_subtype | object_subtype_name |            object_md5            |       processed_time
-------------+-------------+-------------+-------------+------------------+----------------+---------------------+----------------------------------+----------------------------
           1 |           2 | sales       | View        | v_orders_base    |                |                     | 8ab5ff64b11f49649311e714ba77d48c | 2025-11-25 22:35:08.518802
           2 |           2 | sales       | View        | v_orders_report  |                |                     | c09fcd23b41c82a278650e57f84d7dd3 | 2025-11-25 22:35:08.52383
(2 rows)


-- Test Run 2 – Modification (Alter will not work for view, we must use create or replace)

DROP VIEW sales.v_orders_base CASCADE;

-- removed amount column from view

CREATE OR REPLACE VIEW sales.v_orders_base AS
SELECT order_id, customer_name
FROM sales.orders
WHERE amount IS NOT NULL;

    SELECT * FROM pdcd_schema.load_snapshot_tbl();
    
    SELECT * from pdcd_schema.compare_load_md5_metadata_functions(ARRAY['sales','hr']);

    TRUNCATE TABLE pdcd_schema.md5_metadata_staging_functions RESTART IDENTITY CASCADE;
    
    SELECT * from pdcd_schema.load_md5_metadata_staging_functions(ARRAY['sales','hr']);


select metadata_id,snapshot_id,schema_name,object_type,object_type_name,object_subtype,object_subtype_name,object_md5,processed_time,change_type FROM pdcd_schema.md5_metadata_tbl;
          26 |           1 | sales       | View        | v_orders_base    |                |                     | 1e6b3547c6b4cb323d9f1e21d4ecec0f | 2025-11-25 22:31:54.517711 | ADDED
          27 |           1 | sales       | View        | v_orders_report  |                |                     | c09fcd23b41c82a278650e57f84d7dd3 | 2025-11-25 22:31:54.517741 | ADDED
          28 |           2 | sales       | View        | v_orders_base    |                |                     | 8ab5ff64b11f49649311e714ba77d48c | 2025-11-25 22:35:08.461134 | MODIFIED
          29 |           3 | sales       | View        | v_orders_base    |                |                     | e991abf87f0bdf2bb5ec3dffa12e122d | 2025-11-25 22:38:51.293484 | MODIFIED
          30 |           3 | sales       | View        | v_orders_report  |                |                     | c09fcd23b41c82a278650e57f84d7dd3 | 2025-11-25 22:38:51.293631 | DELETED
(30 rows)

-- v_orders_report is deleted because it depends on v_orders_base which is modified to remove amount column

 select metadata_id,snapshot_id,schema_name,object_type,object_type_name,object_subtype,object_subtype_name,object_md5,processed_time FROM pdcd_schema.md5_metadata_staging_functions;
 metadata_id | snapshot_id | schema_name | object_type | object_type_name | object_subtype | object_subtype_name |            object_md5            |       processed_time
-------------+-------------+-------------+-------------+------------------+----------------+---------------------+----------------------------------+----------------------------
           1 |           3 | sales       | View        | v_orders_base    |                |                     | e991abf87f0bdf2bb5ec3dffa12e122d | 2025-11-25 22:38:51.319365
(1 row)

-- Test Run 3 – Renaming and Adding Views

ALTER VIEW sales.v_orders_base RENAME TO v_orders_master;

CREATE VIEW sales.v_orders_summary AS
SELECT customer_name
FROM sales.v_orders_master
GROUP BY customer_name;

    SELECT * FROM pdcd_schema.load_snapshot_tbl();
    
    SELECT * from pdcd_schema.compare_load_md5_metadata_functions(ARRAY['sales','hr']);
    
    TRUNCATE TABLE pdcd_schema.md5_metadata_staging_functions RESTART IDENTITY CASCADE;
    
    SELECT * from pdcd_schema.load_md5_metadata_staging_functions(ARRAY['sales','hr']);

select metadata_id,snapshot_id,schema_name,object_type,object_type_name,object_subtype,object_subtype_name,object_md5,processed_time,change_type FROM pdcd_schema.md5_metadata_tbl;
 metadata_id | snapshot_id | schema_name | object_type | object_type_name | object_subtype | object_subtype_name |            object_md5            |       processed_time       | change_type

          26 |           1 | sales       | View        | v_orders_base    |                |                     | 1e6b3547c6b4cb323d9f1e21d4ecec0f | 2025-11-25 22:31:54.517711 | ADDED
          27 |           1 | sales       | View        | v_orders_report  |                |                     | c09fcd23b41c82a278650e57f84d7dd3 | 2025-11-25 22:31:54.517741 | ADDED
          28 |           2 | sales       | View        | v_orders_base    |                |                     | 8ab5ff64b11f49649311e714ba77d48c | 2025-11-25 22:35:08.461134 | MODIFIED
          29 |           3 | sales       | View        | v_orders_base    |                |                     | e991abf87f0bdf2bb5ec3dffa12e122d | 2025-11-25 22:38:51.293484 | MODIFIED
          30 |           3 | sales       | View        | v_orders_report  |                |                     | c09fcd23b41c82a278650e57f84d7dd3 | 2025-11-25 22:38:51.293631 | DELETED
          31 |           4 | sales       | View        | v_orders_master  |                |                     | e991abf87f0bdf2bb5ec3dffa12e122d | 2025-11-25 22:45:25.496643 | RENAMED
          32 |           4 | sales       | View        | v_orders_summary |                |                     | eae65840ba912c68f01972ce81752149 | 2025-11-25 22:45:25.500173 | ADDED
(32 rows)

select metadata_id,snapshot_id,schema_name,object_type,object_type_name,object_subtype,object_subtype_name,object_md5,processed_time FROM pdcd_schema.md5_metadata_staging_functions;
 metadata_id | snapshot_id | schema_name | object_type | object_type_name | object_subtype | object_subtype_name |            object_md5            |       processed_time
-------------+-------------+-------------+-------------+------------------+----------------+---------------------+----------------------------------+----------------------------
           1 |           4 | sales       | View        | v_orders_master  |                |                     | e991abf87f0bdf2bb5ec3dffa12e122d | 2025-11-25 22:45:25.532206
           2 |           4 | sales       | View        | v_orders_summary |                |                     | eae65840ba912c68f01972ce81752149 | 2025-11-25 22:45:25.540273
(2 rows)


-- Test 4 -- view defination change

CREATE OR REPLACE VIEW sales.v_orders_master AS
SELECT order_id, customer_name
FROM sales.orders;

    SELECT * FROM pdcd_schema.load_snapshot_tbl();
    
    SELECT * from pdcd_schema.compare_load_md5_metadata_functions(ARRAY['sales','hr']);
    
    TRUNCATE TABLE pdcd_schema.md5_metadata_staging_functions RESTART IDENTITY CASCADE;
    
    SELECT * from pdcd_schema.load_md5_metadata_staging_functions(ARRAY['sales','hr']);

select metadata_id,snapshot_id,schema_name,object_type,object_type_name,object_subtype,object_subtype_name,object_md5,processed_time,change_type FROM pdcd_schema.md5_metadata_tbl;
          26 |           1 | sales       | View        | v_orders_base    |                |                     | 1e6b3547c6b4cb323d9f1e21d4ecec0f | 2025-11-25 22:31:54.517711 | ADDED
          27 |           1 | sales       | View        | v_orders_report  |                |                     | c09fcd23b41c82a278650e57f84d7dd3 | 2025-11-25 22:31:54.517741 | ADDED
          28 |           2 | sales       | View        | v_orders_base    |                |                     | 8ab5ff64b11f49649311e714ba77d48c | 2025-11-25 22:35:08.461134 | MODIFIED
          29 |           3 | sales       | View        | v_orders_base    |                |                     | e991abf87f0bdf2bb5ec3dffa12e122d | 2025-11-25 22:38:51.293484 | MODIFIED
          30 |           3 | sales       | View        | v_orders_report  |                |                     | c09fcd23b41c82a278650e57f84d7dd3 | 2025-11-25 22:38:51.293631 | DELETED
          31 |           4 | sales       | View        | v_orders_master  |                |                     | e991abf87f0bdf2bb5ec3dffa12e122d | 2025-11-25 22:45:25.496643 | RENAMED
          32 |           4 | sales       | View        | v_orders_summary |                |                     | eae65840ba912c68f01972ce81752149 | 2025-11-25 22:45:25.500173 | ADDED
          33 |           5 | sales       | View        | v_orders_master  |                |                     | 8ade1ed7191ba1ff31a0d75a16c67096 | 2025-11-25 22:47:08.801464 | MODIFIED
(33 rows)

select metadata_id,snapshot_id,schema_name,object_type,object_type_name,object_subtype,object_subtype_name,object_md5,processed_time FROM pdcd_schema.md5_metadata_staging_functions;
 metadata_id | snapshot_id | schema_name | object_type | object_type_name | object_subtype | object_subtype_name |            object_md5            |       processed_time
-------------+-------------+-------------+-------------+------------------+----------------+---------------------+----------------------------------+----------------------------
           1 |           5 | sales       | View        | v_orders_summary |                |                     | eae65840ba912c68f01972ce81752149 | 2025-11-25 22:47:08.834785
           2 |           5 | sales       | View        | v_orders_master  |                |                     | 8ade1ed7191ba1ff31a0d75a16c67096 | 2025-11-25 22:47:08.841523
(2 rows)

 -- Test 5 –Drop Views & Add New View

DROP VIEW sales.v_orders_summary;

CREATE VIEW sales.v_orders_summary AS
SELECT customer_name, COUNT(order_id) AS total_orders
FROM sales.v_orders_master
GROUP BY customer_name;

    SELECT * FROM pdcd_schema.load_snapshot_tbl();
    
    SELECT * from pdcd_schema.compare_load_md5_metadata_functions(ARRAY['sales','hr']);
    
    TRUNCATE TABLE pdcd_schema.md5_metadata_staging_functions RESTART IDENTITY CASCADE;
    
    SELECT * from pdcd_schema.load_md5_metadata_staging_functions(ARRAY['sales','hr']);

select metadata_id,snapshot_id,schema_name,object_type,object_type_name,object_subtype,object_subtype_name,object_md5,processed_time,change_type FROM pdcd_schema.md5_metadata_tbl;
 metadata_id | snapshot_id | schema_name | object_type | object_type_name | object_subtype | object_subtype_name |            object_md5            |       processed_time       | change_type
          26 |           1 | sales       | View        | v_orders_base    |                |                     | 1e6b3547c6b4cb323d9f1e21d4ecec0f | 2025-11-25 22:31:54.517711 | ADDED
          27 |           1 | sales       | View        | v_orders_report  |                |                     | c09fcd23b41c82a278650e57f84d7dd3 | 2025-11-25 22:31:54.517741 | ADDED
          28 |           2 | sales       | View        | v_orders_base    |                |                     | 8ab5ff64b11f49649311e714ba77d48c | 2025-11-25 22:35:08.461134 | MODIFIED
          29 |           3 | sales       | View        | v_orders_base    |                |                     | e991abf87f0bdf2bb5ec3dffa12e122d | 2025-11-25 22:38:51.293484 | MODIFIED
          30 |           3 | sales       | View        | v_orders_report  |                |                     | c09fcd23b41c82a278650e57f84d7dd3 | 2025-11-25 22:38:51.293631 | DELETED
          31 |           4 | sales       | View        | v_orders_master  |                |                     | e991abf87f0bdf2bb5ec3dffa12e122d | 2025-11-25 22:45:25.496643 | RENAMED
          32 |           4 | sales       | View        | v_orders_summary |                |                     | eae65840ba912c68f01972ce81752149 | 2025-11-25 22:45:25.500173 | ADDED
          33 |           5 | sales       | View        | v_orders_master  |                |                     | 8ade1ed7191ba1ff31a0d75a16c67096 | 2025-11-25 22:47:08.801464 | MODIFIED
          34 |           6 | sales       | View        | v_orders_summary |                |                     | 00f9b1830b32611113eb4bd8bf039d04 | 2025-11-25 22:48:49.43769  | MODIFIED
(34 rows)

select metadata_id,snapshot_id,schema_name,object_type,object_type_name,object_subtype,object_subtype_name,object_md5,processed_time FROM pdcd_schema.md5_metadata_staging_functions;
 metadata_id | snapshot_id | schema_name | object_type | object_type_name | object_subtype | object_subtype_name |            object_md5            |       processed_time
-------------+-------------+-------------+-------------+------------------+----------------+---------------------+----------------------------------+----------------------------
           1 |           6 | sales       | View        | v_orders_summary |                |                     | 00f9b1830b32611113eb4bd8bf039d04 | 2025-11-25 22:48:49.465125
           2 |           6 | sales       | View        | v_orders_master  |                |                     | 8ade1ed7191ba1ff31a0d75a16c67096 | 2025-11-25 22:48:49.473245
(2 rows)


-- Test 6 – Mixed changes

-- ALTER VIEW sales.v_orders_report RENAME TO v_orders_high_value;
-- DROP VIEW sales.v_orders_master;
-- CREATE VIEW sales.v_orders_master AS
-- SELECT order_id, customer_name, amount
-- FROM sales.orders;

ALTER VIEW sales.v_orders_report RENAME TO v_orders_high_value;

-- Fix Error 2: Drop the existing master view and dependent views using CASCADE
DROP VIEW sales.v_orders_master CASCADE;

-- Fix Error 3: Recreate the master view using CREATE OR REPLACE to avoid "already exists" errors
CREATE OR REPLACE VIEW sales.v_orders_master AS
SELECT order_id, customer_name, amount
FROM sales.orders;

    SELECT * FROM pdcd_schema.load_snapshot_tbl();
    
    SELECT * from pdcd_schema.compare_load_md5_metadata_functions(ARRAY['sales','hr']);
    
    TRUNCATE TABLE pdcd_schema.md5_metadata_staging_functions RESTART IDENTITY CASCADE;
    
    SELECT * from pdcd_schema.load_md5_metadata_staging_functions(ARRAY['sales','hr']);

select metadata_id,snapshot_id,schema_name,object_type,object_type_name,object_subtype,object_subtype_name,object_md5,processed_time,change_type FROM pdcd_schema.md5_metadata_tbl;
 metadata_id | snapshot_id | schema_name | object_type | object_type_name | object_subtype | object_subtype_name |            object_md5            |       processed_time       | change_type
          26 |           1 | sales       | View        | v_orders_base    |                |                     | 1e6b3547c6b4cb323d9f1e21d4ecec0f | 2025-11-25 22:31:54.517711 | ADDED
          27 |           1 | sales       | View        | v_orders_report  |                |                     | c09fcd23b41c82a278650e57f84d7dd3 | 2025-11-25 22:31:54.517741 | ADDED
          28 |           2 | sales       | View        | v_orders_base    |                |                     | 8ab5ff64b11f49649311e714ba77d48c | 2025-11-25 22:35:08.461134 | MODIFIED
          29 |           3 | sales       | View        | v_orders_base    |                |                     | e991abf87f0bdf2bb5ec3dffa12e122d | 2025-11-25 22:38:51.293484 | MODIFIED
          30 |           3 | sales       | View        | v_orders_report  |                |                     | c09fcd23b41c82a278650e57f84d7dd3 | 2025-11-25 22:38:51.293631 | DELETED
          31 |           4 | sales       | View        | v_orders_master  |                |                     | e991abf87f0bdf2bb5ec3dffa12e122d | 2025-11-25 22:45:25.496643 | RENAMED
          32 |           4 | sales       | View        | v_orders_summary |                |                     | eae65840ba912c68f01972ce81752149 | 2025-11-25 22:45:25.500173 | ADDED
          33 |           5 | sales       | View        | v_orders_master  |                |                     | 8ade1ed7191ba1ff31a0d75a16c67096 | 2025-11-25 22:47:08.801464 | MODIFIED
          34 |           6 | sales       | View        | v_orders_summary |                |                     | 00f9b1830b32611113eb4bd8bf039d04 | 2025-11-25 22:48:49.43769  | MODIFIED
          35 |           7 | sales       | View        | v_orders_master  |                |                     | 1e6b3547c6b4cb323d9f1e21d4ecec0f | 2025-11-25 23:02:55.260074 | MODIFIED
          36 |           7 | sales       | View        | v_orders_summary |                |                     | 00f9b1830b32611113eb4bd8bf039d04 | 2025-11-25 23:02:55.260248 | DELETED
(36 rows)

select metadata_id,snapshot_id,schema_name,object_type,object_type_name,object_subtype,object_subtype_name,object_md5,processed_time FROM pdcd_schema.md5_metadata_staging_functions;
 metadata_id | snapshot_id | schema_name | object_type | object_type_name | object_subtype | object_subtype_name |            object_md5            |       processed_time
-------------+-------------+-------------+-------------+------------------+----------------+---------------------+----------------------------------+----------------------------
           1 |           7 | sales       | View        | v_orders_master  |                |                     | 1e6b3547c6b4cb323d9f1e21d4ecec0f | 2025-11-25 23:02:55.292617
(1 row)


-- Test Run 7 — INSTEAD OF Trigger on View

CREATE OR REPLACE FUNCTION sales.trg_insert_orders()
RETURNS trigger AS $$
BEGIN
    INSERT INTO sales.orders(order_id, customer_name, amount)
    VALUES (NEW.order_id, NEW.customer_name, NEW.amount);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_orders_insert
INSTEAD OF INSERT ON sales.v_orders_master
FOR EACH ROW EXECUTE FUNCTION sales.trg_insert_orders();

    SELECT * FROM pdcd_schema.load_snapshot_tbl();
    
    SELECT * from pdcd_schema.compare_load_md5_metadata_functions(ARRAY['sales','hr']);
    
    TRUNCATE TABLE pdcd_schema.md5_metadata_staging_functions RESTART IDENTITY CASCADE;
    
    SELECT * from pdcd_schema.load_md5_metadata_staging_functions(ARRAY['sales','hr']);

          26 |           1 | sales       | View        | v_orders_base     |                |                     | 1e6b3547c6b4cb323d9f1e21d4ecec0f | 2025-11-25 22:31:54.517711 | ADDED
          27 |           1 | sales       | View        | v_orders_report   |                |                     | c09fcd23b41c82a278650e57f84d7dd3 | 2025-11-25 22:31:54.517741 | ADDED
          28 |           2 | sales       | View        | v_orders_base     |                |                     | 8ab5ff64b11f49649311e714ba77d48c | 2025-11-25 22:35:08.461134 | MODIFIED
          29 |           3 | sales       | View        | v_orders_base     |                |                     | e991abf87f0bdf2bb5ec3dffa12e122d | 2025-11-25 22:38:51.293484 | MODIFIED
          30 |           3 | sales       | View        | v_orders_report   |                |                     | c09fcd23b41c82a278650e57f84d7dd3 | 2025-11-25 22:38:51.293631 | DELETED
          31 |           4 | sales       | View        | v_orders_master   |                |                     | e991abf87f0bdf2bb5ec3dffa12e122d | 2025-11-25 22:45:25.496643 | RENAMED
          32 |           4 | sales       | View        | v_orders_summary  |                |                     | eae65840ba912c68f01972ce81752149 | 2025-11-25 22:45:25.500173 | ADDED
          33 |           5 | sales       | View        | v_orders_master   |                |                     | 8ade1ed7191ba1ff31a0d75a16c67096 | 2025-11-25 22:47:08.801464 | MODIFIED
          34 |           6 | sales       | View        | v_orders_summary  |                |                     | 00f9b1830b32611113eb4bd8bf039d04 | 2025-11-25 22:48:49.43769  | MODIFIED
          35 |           7 | sales       | View        | v_orders_master   |                |                     | 1e6b3547c6b4cb323d9f1e21d4ecec0f | 2025-11-25 23:02:55.260074 | MODIFIED
          36 |           7 | sales       | View        | v_orders_summary  |                |                     | 00f9b1830b32611113eb4bd8bf039d04 | 2025-11-25 23:02:55.260248 | DELETED
          37 |           8 | sales       | View        | v_orders_master   |                |                     | c6e6de91ce0e1b084f44044bf97b1d29 | 2025-11-25 23:06:09.546229 | MODIFIED
          38 |           8 | sales       | Function    | trg_insert_orders |                |                     | cb9869f35b69132a902aff7aabad5111 | 2025-11-25 23:06:09.546325 | ADDED
(38 rows)

select metadata_id,snapshot_id,schema_name,object_type,object_type_name,object_subtype,object_subtype_name,object_md5,processed_time FROM pdcd_schema.md5_metadata_staging_functions;
 metadata_id | snapshot_id | schema_name | object_type | object_type_name  | object_subtype | object_subtype_name |            object_md5            |       processed_time
-------------+-------------+-------------+-------------+-------------------+----------------+---------------------+----------------------------------+----------------------------
           1 |           8 | sales       | Function    | trg_insert_orders |                |                     | cb9869f35b69132a902aff7aabad5111 | 2025-11-25 23:06:14.523219
           2 |           8 | sales       | View        | v_orders_master   |                |                     | c6e6de91ce0e1b084f44044bf97b1d29 | 2025-11-25 23:06:14.544535
(2 rows)


-- Test Run 8 — Dependent View Change

CREATE OR REPLACE VIEW sales.v_orders_high_value AS
SELECT order_id, customer_name
FROM sales.v_orders_master
WHERE amount > 5000;

    SELECT * FROM pdcd_schema.load_snapshot_tbl();
    
    SELECT * from pdcd_schema.compare_load_md5_metadata_functions(ARRAY['sales','hr']);
    
    TRUNCATE TABLE pdcd_schema.md5_metadata_staging_functions RESTART IDENTITY CASCADE;
    
    SELECT * from pdcd_schema.load_md5_metadata_staging_functions(ARRAY['sales','hr']);

select metadata_id,snapshot_id,schema_name,object_type,object_type_name,object_subtype,object_subtype_name,object_md5,processed_time,change_type FROM pdcd_schema.md5_metadata_tbl;
          35 |           7 | sales       | View        | v_orders_master     |                |                     | 1e6b3547c6b4cb323d9f1e21d4ecec0f | 2025-11-25 23:02:55.260074 | MODIFIED
          36 |           7 | sales       | View        | v_orders_summary    |                |                     | 00f9b1830b32611113eb4bd8bf039d04 | 2025-11-25 23:02:55.260248 | DELETED
          37 |           8 | sales       | View        | v_orders_master     |                |                     | c6e6de91ce0e1b084f44044bf97b1d29 | 2025-11-25 23:06:09.546229 | MODIFIED
          38 |           8 | sales       | Function    | trg_insert_orders   |                |                     | cb9869f35b69132a902aff7aabad5111 | 2025-11-25 23:06:09.546325 | ADDED
          39 |           9 | sales       | View        | v_orders_high_value |                |                     | cb1ecc0097dd547e5787a7a6dee28328 | 2025-11-25 23:07:46.786968 | ADDED
(39 rows)

select metadata_id,snapshot_id,schema_name,object_type,object_type_name,object_subtype,object_subtype_name,object_md5,processed_time FROM pdcd_schema.md5_metadata_staging_functions;
 metadata_id | snapshot_id | schema_name | object_type |  object_type_name   | object_subtype | object_subtype_name |            object_md5            |       processed_time
-------------+-------------+-------------+-------------+---------------------+----------------+---------------------+----------------------------------+----------------------------
           1 |           9 | sales       | Function    | trg_insert_orders   |                |                     | cb9869f35b69132a902aff7aabad5111 | 2025-11-25 23:07:46.8164
           2 |           9 | sales       | View        | v_orders_master     |                |                     | c6e6de91ce0e1b084f44044bf97b1d29 | 2025-11-25 23:07:46.832255
           3 |           9 | sales       | View        | v_orders_high_value |                |                     | cb1ecc0097dd547e5787a7a6dee28328 | 2025-11-25 23:07:46.832288
(3 rows)

-- Test Run 8 — Drop & Recreate Trigger

DROP TRIGGER trg_orders_insert ON sales.v_orders_master;

CREATE TRIGGER trg_orders_insert
INSTEAD OF INSERT ON sales.v_orders_master
FOR EACH ROW EXECUTE FUNCTION sales.trg_insert_orders();

    SELECT * FROM pdcd_schema.load_snapshot_tbl();
    SELECT * FROM pdcd_schema.compare_load_md5_metadata_tbl(ARRAY['sales','hr']);
    SELECT * from pdcd_schema.compare_load_md5_metadata_functions(ARRAY['sales','hr']);
    TRUNCATE TABLE pdcd_schema.md5_metadata_staging_tbl RESTART IDENTITY CASCADE;
    TRUNCATE TABLE pdcd_schema.md5_metadata_staging_functions RESTART IDENTITY CASCADE;
    SELECT * FROM pdcd_schema.load_md5_metadata_staging_tbl(ARRAY['sales','hr']);
    SELECT * from pdcd_schema.load_md5_metadata_staging_functions(ARRAY['sales','hr']);



