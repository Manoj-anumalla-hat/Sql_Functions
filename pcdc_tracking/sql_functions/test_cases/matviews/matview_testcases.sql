TRUNCATE TABLE pdcd_schema.md5_metadata_tbl RESTART IDENTITY CASCADE;
TRUNCATE TABLE pdcd_schema.md5_metadata_staging_tbl RESTART IDENTITY CASCADE;
TRUNCATE TABLE pdcd_schema.md5_metadata_staging_functions RESTART IDENTITY CASCADE;
TRUNCATE TABLE pdcd_schema.snapshot_tbl RESTART IDENTITY CASCADE;


DROP SCHEMA IF EXISTS test_mv CASCADE;


CREATE SCHEMA test_mv;

-- Base tables
CREATE TABLE test_mv.customers (
    cust_id     INT PRIMARY KEY,
    cust_name   VARCHAR(50),
    segment     VARCHAR(20)
);

CREATE TABLE test_mv.sales_txn (
    txn_id      BIGSERIAL PRIMARY KEY,
    cust_id     INT REFERENCES test_mv.customers(cust_id),
    txn_date    DATE,
    amount      NUMERIC(10,2)
);

-- First Run
SELECT * FROM pdcd_schema.load_snapshot_tbl();
SELECT * FROM pdcd_schema.load_md5_metadata_tbl(ARRAY['test_mv']);
SELECT * FROM pdcd_schema.load_md5_metadata_staging_tbl(ARRAY['test_mv']);
SELECT * from pdcd_schema.load_md5_metadata_staging_functions(ARRAY['test_mv']);

select metadata_id,snapshot_id,schema_name,object_type,object_type_name,object_subtype,object_subtype_name,object_md5,processed_time,change_type FROM pdcd_schema.md5_metadata_tbl;
 metadata_id | snapshot_id | schema_name | object_type | object_type_name | object_subtype |  object_subtype_name   |            object_md5            |       processed_time       | change_type
-------------+-------------+-------------+-------------+------------------+----------------+------------------------+----------------------------------+----------------------------+-------------
           1 |           1 | test_mv     | Table       | customers        | Column         | cust_id                | e6c2480669939991ff6fe41c5229279b | 2025-11-26 12:55:38.691962 | ADDED
           2 |           1 | test_mv     | Table       | customers        | Column         | cust_name              | b537a75e4c2744b85e478bf937370ba9 | 2025-11-26 12:55:38.699082 | ADDED
           3 |           1 | test_mv     | Table       | customers        | Column         | segment                | 350fa0710b624e92cd2d3439c54cee88 | 2025-11-26 12:55:38.699103 | ADDED
           4 |           1 | test_mv     | Table       | sales_txn        | Column         | txn_id                 | 85595afcdc233d8b832de65dfc5c9d13 | 2025-11-26 12:55:38.699108 | ADDED
           5 |           1 | test_mv     | Table       | sales_txn        | Column         | cust_id                | 8aa548f7ce8c4f95f6dc839e757d25c0 | 2025-11-26 12:55:38.699113 | ADDED
           6 |           1 | test_mv     | Table       | sales_txn        | Column         | txn_date               | a8872c6b2153479b58da9f8414912303 | 2025-11-26 12:55:38.699118 | ADDED
           7 |           1 | test_mv     | Table       | sales_txn        | Column         | amount                 | 92b69c3ff65f71e899dfd03c9f0cb199 | 2025-11-26 12:55:38.699122 | ADDED
           8 |           1 | test_mv     | Table       | customers        | Constraint     | customers_pkey         | 0af9a8482197dc79fb7e3c157f2371fd | 2025-11-26 12:55:38.701636 | ADDED
           9 |           1 | test_mv     | Table       | sales_txn        | Constraint     | sales_txn_cust_id_fkey | 1c65fb937d94cae364bae3cdc959678d | 2025-11-26 12:55:38.701661 | ADDED
          10 |           1 | test_mv     | Table       | sales_txn        | Constraint     | sales_txn_pkey         | 6843002dcdbadf4d026ef385f319f49e | 2025-11-26 12:55:38.701668 | ADDED
          11 |           1 | test_mv     | Table       | customers        | Index          | customers_pkey         | 6df2c0224eed65273dfcf4a883c27bba | 2025-11-26 12:55:38.704939 | ADDED
          12 |           1 | test_mv     | Table       | sales_txn        | Index          | sales_txn_pkey         | 0432928932bec85010c4c581e9032b86 | 2025-11-26 12:55:38.704959 | ADDED
          13 |           1 | test_mv     | Table       | sales_txn        | Reference      | sales_txn_cust_id_fkey | c11aeff0e53dc6614383720e22b83ac7 | 2025-11-26 12:55:38.710178 | ADDED
          14 |           1 | test_mv     | Table       | sales_txn        | Sequence       | sales_txn_txn_id_seq   | ed48d9f699661cbdf84b30f41cc02dc3 | 2025-11-26 12:55:38.731685 | ADDED
(14 rows)

select metadata_id,snapshot_id,schema_name,object_type,object_type_name,object_subtype,object_subtype_name,object_md5,processed_time from pdcd_schema.md5_metadata_staging_tbl;

-- same output as above but from staging table

select metadata_id,snapshot_id,schema_name,object_type,object_type_name,object_subtype,object_subtype_name,object_md5,processed_time from pdcd_schema.md5_metadata_staging_functions;

-- no functions,views,matviews created yet, so no output

--=================================

--* Test Run 1 — Add Materialized View + Dependent View + Index

-- 1. Create base materialized view
CREATE MATERIALIZED VIEW test_mv.mv_sales_daily AS
SELECT
    txn_date,
    COUNT(*)         AS txn_cnt,
    SUM(amount)      AS total_amount
FROM test_mv.sales_txn
GROUP BY txn_date;

-- 2. Create dependent view on MV
CREATE VIEW test_mv.v_sales_daily_high AS
SELECT
    txn_date,
    txn_cnt,
    total_amount
FROM test_mv.mv_sales_daily
WHERE total_amount > 10000;

-- 3. Create index on MV
CREATE INDEX idx_mv_sales_daily_date
    ON test_mv.mv_sales_daily (txn_date);

    SELECT * FROM pdcd_schema.load_snapshot_tbl();
    SELECT * FROM pdcd_schema.compare_load_md5_metadata_tbl(ARRAY['test_mv']);
    SELECT * from pdcd_schema.compare_load_md5_metadata_functions(ARRAY['test_mv']);
    TRUNCATE TABLE pdcd_schema.md5_metadata_staging_tbl RESTART IDENTITY CASCADE;
    TRUNCATE TABLE pdcd_schema.md5_metadata_staging_functions RESTART IDENTITY CASCADE;
    SELECT * FROM pdcd_schema.load_md5_metadata_staging_tbl(ARRAY['test_mv']);
    SELECT * from pdcd_schema.load_md5_metadata_staging_functions(ARRAY['test_mv']);

select metadata_id,snapshot_id,schema_name,object_type,object_type_name,object_subtype,object_subtype_name,object_md5,processed_time,change_type FROM pdcd_schema.md5_metadata_tbl;
          12 |           1 | test_mv     | Table             | sales_txn          | Index          | sales_txn_pkey          | fd723bbd8a95efe60c59f64e9673c6d1 | 2025-11-26 17:19:01.903103 | ADDED
          13 |           1 | test_mv     | Table             | sales_txn          | Reference      | sales_txn_cust_id_fkey  | c11aeff0e53dc6614383720e22b83ac7 | 2025-11-26 17:19:01.91061  | ADDED
          14 |           1 | test_mv     | Table             | sales_txn          | Sequence       | sales_txn_txn_id_seq    | ed48d9f699661cbdf84b30f41cc02dc3 | 2025-11-26 17:19:01.914166 | ADDED
          15 |           2 | test_mv     | Table             | v_sales_daily_high | Column         | txn_date                | 1efa9c3fa16d52cb8394cce206ad18a3 | 2025-11-26 17:20:22.507781 | ADDED
          16 |           2 | test_mv     | Table             | v_sales_daily_high | Column         | txn_cnt                 | 281220c8e13d3604037c4cb48f84b433 | 2025-11-26 17:20:22.507834 | ADDED
          17 |           2 | test_mv     | Table             | v_sales_daily_high | Column         | total_amount            | a24ea704b07d3430df42fc104309d38d | 2025-11-26 17:20:22.50784  | ADDED
          18 |           2 | test_mv     | Materialized View | mv_sales_daily     | Index          | idx_mv_sales_daily_date | 4d66be207f0c470f85c2109d3340e23c | 2025-11-26 17:20:22.507848 | ADDED
          19 |           2 | test_mv     | View              | v_sales_daily_high |                |                         | 375edbf6210412a63ee4ee70f375da9f | 2025-11-26 17:20:23.874811 | ADDED
          20 |           2 | test_mv     | Materialized View | mv_sales_daily     |                |                         | 2f41c4950f6966e48ea26de31d9c43aa | 2025-11-26 17:20:23.882823 | ADDED
(20 rows)

select metadata_id,snapshot_id,schema_name,object_type,object_type_name,object_subtype,object_subtype_name,object_md5,processed_time from pdcd_schema.md5_metadata_staging_tbl;
 metadata_id | snapshot_id | schema_name |    object_type    |  object_type_name  | object_subtype |   object_subtype_name   |            object_md5            |       processed_time
-------------+-------------+-------------+-------------------+--------------------+----------------+-------------------------+----------------------------------+----------------------------
           1 |           2 | test_mv     | Table             | sales_txn          | Column         | cust_id                 | 8aa548f7ce8c4f95f6dc839e757d25c0 | 2025-11-26 17:20:24.520458
           2 |           2 | test_mv     | Table             | sales_txn          | Column         | txn_date                | a8872c6b2153479b58da9f8414912303 | 2025-11-26 17:20:24.528357
           3 |           2 | test_mv     | Table             | customers          | Column         | cust_name               | b537a75e4c2744b85e478bf937370ba9 | 2025-11-26 17:20:24.528388
           4 |           2 | test_mv     | Table             | sales_txn          | Column         | txn_id                  | 85595afcdc233d8b832de65dfc5c9d13 | 2025-11-26 17:20:24.528396
           5 |           2 | test_mv     | Table             | v_sales_daily_high | Column         | txn_cnt                 | 281220c8e13d3604037c4cb48f84b433 | 2025-11-26 17:20:24.528404
           6 |           2 | test_mv     | Table             | customers          | Column         | cust_id                 | e6c2480669939991ff6fe41c5229279b | 2025-11-26 17:20:24.52841
           7 |           2 | test_mv     | Table             | sales_txn          | Column         | amount                  | 92b69c3ff65f71e899dfd03c9f0cb199 | 2025-11-26 17:20:24.528417
           8 |           2 | test_mv     | Table             | v_sales_daily_high | Column         | txn_date                | 1efa9c3fa16d52cb8394cce206ad18a3 | 2025-11-26 17:20:24.528423
           9 |           2 | test_mv     | Table             | customers          | Column         | segment                 | 350fa0710b624e92cd2d3439c54cee88 | 2025-11-26 17:20:24.528443
          10 |           2 | test_mv     | Table             | v_sales_daily_high | Column         | total_amount            | a24ea704b07d3430df42fc104309d38d | 2025-11-26 17:20:24.52845
          11 |           2 | test_mv     | Table             | sales_txn          | Constraint     | sales_txn_pkey          | 6843002dcdbadf4d026ef385f319f49e | 2025-11-26 17:20:24.531219
          12 |           2 | test_mv     | Table             | customers          | Constraint     | customers_pkey          | 0af9a8482197dc79fb7e3c157f2371fd | 2025-11-26 17:20:24.531229
          13 |           2 | test_mv     | Table             | sales_txn          | Constraint     | sales_txn_cust_id_fkey  | 1c65fb937d94cae364bae3cdc959678d | 2025-11-26 17:20:24.531232
          14 |           2 | test_mv     | Table             | sales_txn          | Index          | sales_txn_pkey          | fd723bbd8a95efe60c59f64e9673c6d1 | 2025-11-26 17:20:24.533896
          15 |           2 | test_mv     | Table             | customers          | Index          | customers_pkey          | 732a1593e99980321e70519dac105e49 | 2025-11-26 17:20:24.533907
          16 |           2 | test_mv     | Materialized View | mv_sales_daily     | Index          | idx_mv_sales_daily_date | 4d66be207f0c470f85c2109d3340e23c | 2025-11-26 17:20:24.533913
          17 |           2 | test_mv     | Table             | sales_txn          | Reference      | sales_txn_cust_id_fkey  | c11aeff0e53dc6614383720e22b83ac7 | 2025-11-26 17:20:24.538749
          18 |           2 | test_mv     | Table             | sales_txn          | Sequence       | sales_txn_txn_id_seq    | ed48d9f699661cbdf84b30f41cc02dc3 | 2025-11-26 17:20:24.541086
(18 rows)

select metadata_id,snapshot_id,schema_name,object_type,object_type_name,object_subtype,object_subtype_name,object_subtype_details,object_md5,processed_time from pdcd_schema.md5_metadata_staging_functions;
 metadata_id | snapshot_id | schema_name |    object_type    |  object_type_name  | object_subtype | object_subtype_name |                                                                 object_subtype_details                                                                  |            object_md5            |       processed_time
-------------+-------------+-------------+-------------------+--------------------+----------------+---------------------+---------------------------------------------------------------------------------------------------------------------------------------------------------+----------------------------------+----------------------------
           1 |           2 | test_mv     | View              | v_sales_daily_high |                |                     | view_type:VIEW,view_definition: SELECT mv_sales_daily.txn_date,                                                                                        +| 375edbf6210412a63ee4ee70f375da9f | 2025-11-26 17:20:25.283852
             |             |             |                   |                    |                |                     |     mv_sales_daily.txn_cnt,                                                                                                                            +|                                  |
             |             |             |                   |                    |                |                     |     mv_sales_daily.total_amount                                                                                                                        +|                                  |
             |             |             |                   |                    |                |                     |    FROM test_mv.mv_sales_daily                                                                                                                         +|                                  |
             |             |             |                   |                    |                |                     |   WHERE mv_sales_daily.total_amount > 10000::numeric;,base_tables:,view_owner:test_user,dependent_objects:Dependent views: test_mv.mv_sales_daily       |                                  |
           2 |           2 | test_mv     | Materialized View | mv_sales_daily     |                |                     | view_type:MATERIALIZED VIEW,view_definition: SELECT sales_txn.txn_date,                                                                                +| 2f41c4950f6966e48ea26de31d9c43aa | 2025-11-26 17:20:25.301528
             |             |             |                   |                    |                |                     |     count(*) AS txn_cnt,                                                                                                                               +|                                  |
             |             |             |                   |                    |                |                     |     sum(sales_txn.amount) AS total_amount                                                                                                              +|                                  |
             |             |             |                   |                    |                |                     |    FROM test_mv.sales_txn                                                                                                                              +|                                  |
             |             |             |                   |                    |                |                     |   GROUP BY sales_txn.txn_date;,base_tables:test_mv.sales_txn,is_ populated:true,view_owner:test_user,dependent_objects:Indexes: idx_mv_sales_daily_date |                                  |
(2 rows)

-- Check index attached to MV
-- In psql:
-- \d+ test_mv.mv_sales_daily

--=================================

--* Test Run 2 — Rename Materialized View (with dependencies)

-- 1. Rename MV
ALTER MATERIALIZED VIEW test_mv.mv_sales_daily
    RENAME TO mv_sales_daily_v1;

-- 2. Validate: dependent view should still work
-- SELECT * FROM test_mv.v_sales_daily_high ORDER BY txn_date;

-- 3. Check index still exists and is bound to new name
-- \d+ test_mv.mv_sales_daily_v1

    SELECT * FROM pdcd_schema.load_snapshot_tbl();
    SELECT * FROM pdcd_schema.compare_load_md5_metadata_tbl(ARRAY['test_mv']);
    SELECT * from pdcd_schema.compare_load_md5_metadata_functions(ARRAY['test_mv']);
    TRUNCATE TABLE pdcd_schema.md5_metadata_staging_tbl RESTART IDENTITY CASCADE;
    TRUNCATE TABLE pdcd_schema.md5_metadata_staging_functions RESTART IDENTITY CASCADE;
    SELECT * FROM pdcd_schema.load_md5_metadata_staging_tbl(ARRAY['test_mv']);
    SELECT * from pdcd_schema.load_md5_metadata_staging_functions(ARRAY['test_mv']);

select metadata_id,snapshot_id,schema_name,object_type,object_type_name,object_subtype,object_subtype_name,object_md5,processed_time,change_type FROM pdcd_schema.md5_metadata_tbl;
          20 |           2 | test_mv     | Materialized View | mv_sales_daily     |                |                         | 2f41c4950f6966e48ea26de31d9c43aa | 2025-11-26 17:20:23.882823 | ADDED
          21 |           3 | test_mv     | Materialized View | mv_sales_daily_v1  | Index          | idx_mv_sales_daily_date | 3c762f0e1110ef7ebf2ca90c1310d817 | 2025-11-26 17:24:11.320075 | ADDED
          22 |           3 | test_mv     | Materialized View | mv_sales_daily     | Index          | idx_mv_sales_daily_date | 4d66be207f0c470f85c2109d3340e23c | 2025-11-26 17:24:11.320153 | DELETED
          23 |           3 | test_mv     | Materialized View | mv_sales_daily_v1  |                |                         | 2f41c4950f6966e48ea26de31d9c43aa | 2025-11-26 17:24:15.070157 | RENAMED
          24 |           3 | test_mv     | View              | v_sales_daily_high |                |                         | 65ce63df26ea6c910e0c96f40790d2c2 | 2025-11-26 17:24:15.07025  | MODIFIED
(24 rows)

select metadata_id,snapshot_id,schema_name,object_type,object_type_name,object_subtype,object_subtype_name,object_md5,processed_time from pdcd_schema.md5_metadata_staging_tbl;
 metadata_id | snapshot_id | schema_name |    object_type    |  object_type_name  | object_subtype |   object_subtype_name   |            object_md5            |       processed_time
-------------+-------------+-------------+-------------------+--------------------+----------------+-------------------------+----------------------------------+----------------------------
           1 |           3 | test_mv     | Table             | sales_txn          | Column         | cust_id                 | 8aa548f7ce8c4f95f6dc839e757d25c0 | 2025-11-26 17:24:15.11291
           2 |           3 | test_mv     | Table             | sales_txn          | Column         | txn_date                | a8872c6b2153479b58da9f8414912303 | 2025-11-26 17:24:15.118247
           3 |           3 | test_mv     | Table             | customers          | Column         | cust_name               | b537a75e4c2744b85e478bf937370ba9 | 2025-11-26 17:24:15.118252
           4 |           3 | test_mv     | Table             | sales_txn          | Column         | txn_id                  | 85595afcdc233d8b832de65dfc5c9d13 | 2025-11-26 17:24:15.118254
           5 |           3 | test_mv     | Table             | v_sales_daily_high | Column         | txn_cnt                 | 281220c8e13d3604037c4cb48f84b433 | 2025-11-26 17:24:15.118256
           6 |           3 | test_mv     | Table             | customers          | Column         | cust_id                 | e6c2480669939991ff6fe41c5229279b | 2025-11-26 17:24:15.118258
           7 |           3 | test_mv     | Table             | sales_txn          | Column         | amount                  | 92b69c3ff65f71e899dfd03c9f0cb199 | 2025-11-26 17:24:15.11826
           8 |           3 | test_mv     | Table             | v_sales_daily_high | Column         | txn_date                | 1efa9c3fa16d52cb8394cce206ad18a3 | 2025-11-26 17:24:15.118262
           9 |           3 | test_mv     | Table             | customers          | Column         | segment                 | 350fa0710b624e92cd2d3439c54cee88 | 2025-11-26 17:24:15.118264
          10 |           3 | test_mv     | Table             | v_sales_daily_high | Column         | total_amount            | a24ea704b07d3430df42fc104309d38d | 2025-11-26 17:24:15.118266
          11 |           3 | test_mv     | Table             | sales_txn          | Constraint     | sales_txn_pkey          | 6843002dcdbadf4d026ef385f319f49e | 2025-11-26 17:24:15.11908
          12 |           3 | test_mv     | Table             | customers          | Constraint     | customers_pkey          | 0af9a8482197dc79fb7e3c157f2371fd | 2025-11-26 17:24:15.119084
          13 |           3 | test_mv     | Table             | sales_txn          | Constraint     | sales_txn_cust_id_fkey  | 1c65fb937d94cae364bae3cdc959678d | 2025-11-26 17:24:15.119087
          14 |           3 | test_mv     | Table             | sales_txn          | Index          | sales_txn_pkey          | fd723bbd8a95efe60c59f64e9673c6d1 | 2025-11-26 17:24:15.120305
          15 |           3 | test_mv     | Table             | customers          | Index          | customers_pkey          | 732a1593e99980321e70519dac105e49 | 2025-11-26 17:24:15.12031
          16 |           3 | test_mv     | Materialized View | mv_sales_daily_v1  | Index          | idx_mv_sales_daily_date | 3c762f0e1110ef7ebf2ca90c1310d817 | 2025-11-26 17:24:15.120312
          17 |           3 | test_mv     | Table             | sales_txn          | Reference      | sales_txn_cust_id_fkey  | c11aeff0e53dc6614383720e22b83ac7 | 2025-11-26 17:24:15.12228
          18 |           3 | test_mv     | Table             | sales_txn          | Sequence       | sales_txn_txn_id_seq    | ed48d9f699661cbdf84b30f41cc02dc3 | 2025-11-26 17:24:15.123218
(18 rows)

select metadata_id,snapshot_id,schema_name,object_type,object_type_name,object_subtype,object_subtype_name,object_subtype_details,object_md5,processed_time from pdcd_schema.md5_metadata_staging_functions;

 metadata_id | snapshot_id | schema_name |    object_type    |  object_type_name  | object_subtype | object_subtype_name |                                                                 object_subtype_details                                                                  |            object_md5            |       processed_time
-------------+-------------+-------------+-------------------+--------------------+----------------+---------------------+---------------------------------------------------------------------------------------------------------------------------------------------------------+----------------------------------+----------------------------
           1 |           3 | test_mv     | View              | v_sales_daily_high |                |                     | view_type:VIEW,view_definition: SELECT mv_sales_daily_v1.txn_date,                                                                                     +| 65ce63df26ea6c910e0c96f40790d2c2 | 2025-11-26 17:24:15.667611
             |             |             |                   |                    |                |                     |     mv_sales_daily_v1.txn_cnt,                                                                                                                         +|                                  |
             |             |             |                   |                    |                |                     |     mv_sales_daily_v1.total_amount                                                                                                                     +|                                  |
             |             |             |                   |                    |                |                     |    FROM test_mv.mv_sales_daily_v1                                                                                                                      +|                                  |
             |             |             |                   |                    |                |                     |   WHERE mv_sales_daily_v1.total_amount > 10000::numeric;,base_tables:,view_owner:test_user,dependent_objects:Dependent views: test_mv.mv_sales_daily_v1 |                                  |
           2 |           3 | test_mv     | Materialized View | mv_sales_daily_v1  |                |                     | view_type:MATERIALIZED VIEW,view_definition: SELECT sales_txn.txn_date,                                                                                +| 2f41c4950f6966e48ea26de31d9c43aa | 2025-11-26 17:24:15.686347
             |             |             |                   |                    |                |                     |     count(*) AS txn_cnt,                                                                                                                               +|                                  |
             |             |             |                   |                    |                |                     |     sum(sales_txn.amount) AS total_amount                                                                                                              +|                                  |
             |             |             |                   |                    |                |                     |    FROM test_mv.sales_txn                                                                                                                              +|                                  |
             |             |             |                   |                    |                |                     |   GROUP BY sales_txn.txn_date;,base_tables:test_mv.sales_txn,is_ populated:true,view_owner:test_user,dependent_objects:Indexes: idx_mv_sales_daily_date |                                  |
(2 rows)

--===============================================
--* Test Run 3 — Modify MV Definition (safe pattern using new MV + swap)

-- PostgreSQL does not support CREATE OR REPLACE MATERIALIZED VIEW.
-- To “modify” definition safely, we create a new MV and swap names.

-- 1. Create a NEW version with changed definition
CREATE MATERIALIZED VIEW test_mv.mv_sales_daily_v2 AS
SELECT
    s.txn_date,
    c.segment,
    COUNT(*)    AS txn_cnt,
    SUM(s.amount) AS total_amount
FROM test_mv.sales_txn s
JOIN test_mv.customers c ON c.cust_id = s.cust_id
GROUP BY s.txn_date, c.segment; 

-- 2. Refresh new MV if needed (here it’s just created, so already fresh)
-- REFRESH MATERIALIZED VIEW test_mv.mv_sales_daily_v2;

DROP VIEW IF EXISTS test_mv.v_sales_daily_high; -- deleted

-- 3. Adjust dependent view to use new MV definition
-- throughing error when we try to alter view, so dropping and recreating
CREATE OR REPLACE VIEW test_mv.v_sales_daily_high AS
SELECT
    txn_date, 
    segment, -- added
    txn_cnt, -- modified
    total_amount -- modified
FROM test_mv.mv_sales_daily_v2
WHERE total_amount > 10000; -- modified

-- 4. Drop old MV (v1) and its index
DROP MATERIALIZED VIEW test_mv.mv_sales_daily_v1 CASCADE;  -- CASCADE will drop idx_mv_sales_daily_date -- deleted


-- 5. Optionally, rename v2 back to canonical name
ALTER MATERIALIZED VIEW test_mv.mv_sales_daily_v2
    RENAME TO mv_sales_daily; -- Added 

-- 6. Recreate index on new MV
CREATE INDEX idx_mv_sales_daily_date
    ON test_mv.mv_sales_daily (txn_date); -- deleted & added  

-- tested modify + dependent view change + index recreation.

    SELECT * FROM pdcd_schema.load_snapshot_tbl();
    SELECT * FROM pdcd_schema.compare_load_md5_metadata_tbl(ARRAY['test_mv']);
    SELECT * from pdcd_schema.compare_load_md5_metadata_functions(ARRAY['test_mv']);
    TRUNCATE TABLE pdcd_schema.md5_metadata_staging_tbl RESTART IDENTITY CASCADE;
    TRUNCATE TABLE pdcd_schema.md5_metadata_staging_functions RESTART IDENTITY CASCADE;
    SELECT * FROM pdcd_schema.load_md5_metadata_staging_tbl(ARRAY['test_mv']);
    SELECT * from pdcd_schema.load_md5_metadata_staging_functions(ARRAY['test_mv']);


select metadata_id,snapshot_id,schema_name,object_type,object_type_name,object_subtype,object_subtype_name,object_md5,processed_time,change_type FROM pdcd_schema.md5_metadata_tbl;
          23 |           3 | test_mv     | Materialized View | mv_sales_daily_v1  |                |                         | 2f41c4950f6966e48ea26de31d9c43aa | 2025-11-26 17:24:15.070157 | RENAMED
          24 |           3 | test_mv     | View              | v_sales_daily_high |                |                         | 65ce63df26ea6c910e0c96f40790d2c2 | 2025-11-26 17:24:15.07025  | MODIFIED
          25 |           4 | test_mv     | Table             | v_sales_daily_high | Column         | txn_cnt                 | 1d53716ad4462a6e22b711ad4e6feb79 | 2025-11-26 17:43:14.243908 | MODIFIED
          26 |           4 | test_mv     | Table             | v_sales_daily_high | Column         | total_amount            | 7fbb864c17b98b7a484d262e78fffe6e | 2025-11-26 17:43:14.243969 | MODIFIED
          27 |           4 | test_mv     | Table             | v_sales_daily_high | Column         | segment                 | ad769f35dcc10b375c867f5b92e6a0ac | 2025-11-26 17:43:14.244017 | ADDED
          28 |           4 | test_mv     | Materialized View | mv_sales_daily     | Index          | idx_mv_sales_daily_date | 4d66be207f0c470f85c2109d3340e23c | 2025-11-26 17:43:14.244027 | ADDED
          29 |           4 | test_mv     | Materialized View | mv_sales_daily_v1  | Index          | idx_mv_sales_daily_date | 3c762f0e1110ef7ebf2ca90c1310d817 | 2025-11-26 17:43:14.244071 | DELETED
          30 |           4 | test_mv     | View              | v_sales_daily_high |                |                         | 076afb6690803013ebf78ed68f0b83de | 2025-11-26 17:43:25.779527 | MODIFIED
          31 |           4 | test_mv     | Materialized View | mv_sales_daily     |                |                         | 6267aad127730a20c5289a8d88395e4b | 2025-11-26 17:43:25.779642 | ADDED
          32 |           4 | test_mv     | Materialized View | mv_sales_daily_v1  |                |                         | 2f41c4950f6966e48ea26de31d9c43aa | 2025-11-26 17:43:25.779677 | DELETED
(32 rows)

select metadata_id,snapshot_id,schema_name,object_type,object_type_name,object_subtype,object_subtype_name,object_md5,processed_time from pdcd_schema.md5_metadata_staging_tbl;
 metadata_id | snapshot_id | schema_name |    object_type    |  object_type_name  | object_subtype |   object_subtype_name   |            object_md5            |       processed_time
-------------+-------------+-------------+-------------------+--------------------+----------------+-------------------------+----------------------------------+----------------------------
           1 |           4 | test_mv     | Table             | sales_txn          | Column         | cust_id                 | 8aa548f7ce8c4f95f6dc839e757d25c0 | 2025-11-26 17:43:25.820022
           2 |           4 | test_mv     | Table             | sales_txn          | Column         | txn_date                | a8872c6b2153479b58da9f8414912303 | 2025-11-26 17:43:25.825077
           3 |           4 | test_mv     | Table             | customers          | Column         | cust_name               | b537a75e4c2744b85e478bf937370ba9 | 2025-11-26 17:43:25.825083
           4 |           4 | test_mv     | Table             | sales_txn          | Column         | txn_id                  | 85595afcdc233d8b832de65dfc5c9d13 | 2025-11-26 17:43:25.825086
           5 |           4 | test_mv     | Table             | customers          | Column         | cust_id                 | e6c2480669939991ff6fe41c5229279b | 2025-11-26 17:43:25.825089
           6 |           4 | test_mv     | Table             | sales_txn          | Column         | amount                  | 92b69c3ff65f71e899dfd03c9f0cb199 | 2025-11-26 17:43:25.825092
           7 |           4 | test_mv     | Table             | v_sales_daily_high | Column         | txn_date                | 1efa9c3fa16d52cb8394cce206ad18a3 | 2025-11-26 17:43:25.825094
           8 |           4 | test_mv     | Table             | customers          | Column         | segment                 | 350fa0710b624e92cd2d3439c54cee88 | 2025-11-26 17:43:25.825097
           9 |           4 | test_mv     | Table             | v_sales_daily_high | Column         | txn_cnt                 | 1d53716ad4462a6e22b711ad4e6feb79 | 2025-11-26 17:43:25.8251
          10 |           4 | test_mv     | Table             | v_sales_daily_high | Column         | segment                 | ad769f35dcc10b375c867f5b92e6a0ac | 2025-11-26 17:43:25.825102
          11 |           4 | test_mv     | Table             | v_sales_daily_high | Column         | total_amount            | 7fbb864c17b98b7a484d262e78fffe6e | 2025-11-26 17:43:25.825111
          12 |           4 | test_mv     | Table             | sales_txn          | Constraint     | sales_txn_pkey          | 6843002dcdbadf4d026ef385f319f49e | 2025-11-26 17:43:25.826664
          13 |           4 | test_mv     | Table             | customers          | Constraint     | customers_pkey          | 0af9a8482197dc79fb7e3c157f2371fd | 2025-11-26 17:43:25.826674
          14 |           4 | test_mv     | Table             | sales_txn          | Constraint     | sales_txn_cust_id_fkey  | 1c65fb937d94cae364bae3cdc959678d | 2025-11-26 17:43:25.826679
          15 |           4 | test_mv     | Table             | sales_txn          | Index          | sales_txn_pkey          | fd723bbd8a95efe60c59f64e9673c6d1 | 2025-11-26 17:43:25.829573
          16 |           4 | test_mv     | Table             | customers          | Index          | customers_pkey          | 732a1593e99980321e70519dac105e49 | 2025-11-26 17:43:25.829583
          17 |           4 | test_mv     | Materialized View | mv_sales_daily     | Index          | idx_mv_sales_daily_date | 4d66be207f0c470f85c2109d3340e23c | 2025-11-26 17:43:25.829589
          18 |           4 | test_mv     | Table             | sales_txn          | Reference      | sales_txn_cust_id_fkey  | c11aeff0e53dc6614383720e22b83ac7 | 2025-11-26 17:43:25.834383
          19 |           4 | test_mv     | Table             | sales_txn          | Sequence       | sales_txn_txn_id_seq    | ed48d9f699661cbdf84b30f41cc02dc3 | 2025-11-26 17:43:25.836034
(19 rows)

select metadata_id,snapshot_id,schema_name,object_type,object_type_name,object_subtype,object_subtype_name,object_subtype_details,object_md5,processed_time from pdcd_schema.md5_metadata_staging_functions;

 metadata_id | snapshot_id | schema_name |    object_type    |  object_type_name  | object_subtype | object_subtype_name |                                                                            object_subtype_details                                                                             |            object_md5            |       processed_time
-------------+-------------+-------------+-------------------+--------------------+----------------+---------------------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+----------------------------------+----------------------------
           1 |           4 | test_mv     | View              | v_sales_daily_high |                |                     | view_type:VIEW,view_definition: SELECT mv_sales_daily.txn_date,                                                                                                              +| 076afb6690803013ebf78ed68f0b83de | 2025-11-26 17:43:26.245561
             |             |             |                   |                    |                |                     |     mv_sales_daily.segment,                                                                                                                                                  +|                                  |
             |             |             |                   |                    |                |                     |     mv_sales_daily.txn_cnt,                                                                                                                                                  +|                                  |
             |             |             |                   |                    |                |                     |     mv_sales_daily.total_amount                                                                                                                                              +|                                  |
             |             |             |                   |                    |                |                     |    FROM test_mv.mv_sales_daily                                                                                                                                               +|                                  |
             |             |             |                   |                    |                |                     |   WHERE mv_sales_daily.total_amount > 10000::numeric;,base_tables:,view_owner:test_user,dependent_objects:Dependent views: test_mv.mv_sales_daily                             |                                  |
           2 |           4 | test_mv     | Materialized View | mv_sales_daily     |                |                     | view_type:MATERIALIZED VIEW,view_definition: SELECT s.txn_date,                                                                                                              +| 6267aad127730a20c5289a8d88395e4b | 2025-11-26 17:43:26.261594
             |             |             |                   |                    |                |                     |     c.segment,                                                                                                                                                               +|                                  |
             |             |             |                   |                    |                |                     |     count(*) AS txn_cnt,                                                                                                                                                     +|                                  |
             |             |             |                   |                    |                |                     |     sum(s.amount) AS total_amount                                                                                                                                            +|                                  |
             |             |             |                   |                    |                |                     |    FROM test_mv.sales_txn s                                                                                                                                                  +|                                  |
             |             |             |                   |                    |                |                     |      JOIN test_mv.customers c ON c.cust_id = s.cust_id                                                                                                                       +|                                  |
             |             |             |                   |                    |                |                     |   GROUP BY s.txn_date, c.segment;,base_tables:test_mv.customers, test_mv.sales_txn,is_ populated:true,view_owner:test_user,dependent_objects:Indexes: idx_mv_sales_daily_date |                                  |
(2 rows)

--* Test Run 4 — Delete MV (show error, then proper CASCADE handling)

-- This will fail because v_sales_daily_high depends on the MV
DROP MATERIALIZED VIEW test_mv.mv_sales_daily;
-- Expect: ERROR: cannot drop materialized view because other objects depend on it

-- 1. Drop MV and all dependents
DROP MATERIALIZED VIEW test_mv.mv_sales_daily CASCADE; 

-- At this point:
--   - test_mv.v_sales_daily_high is also dropped 
--   - idx_mv_sales_daily_date is dropped 

-- 2. Recreate MV (maybe with a slightly different definition)
CREATE MATERIALIZED VIEW test_mv.mv_sales_daily AS
SELECT
    txn_date,
    COUNT(*)         AS txn_cnt,
    SUM(amount)      AS total_amount
FROM test_mv.sales_txn
GROUP BY txn_date;

-- 3. Recreate dependent view
CREATE VIEW test_mv.v_sales_daily_high AS
SELECT
    txn_date,
    txn_cnt,
    total_amount
FROM test_mv.mv_sales_daily
WHERE total_amount > 5000;   -- changed threshold: part of “modify”

-- 4. Recreate index
CREATE INDEX idx_mv_sales_daily_date
    ON test_mv.mv_sales_daily (txn_date); -- no change

-- 5. Validation
-- SELECT * FROM test_mv.mv_sales_daily ORDER BY txn_date;
-- SELECT * FROM test_mv.v_sales_daily_high ORDER BY txn_date;

    SELECT * FROM pdcd_schema.load_snapshot_tbl();
    SELECT * FROM pdcd_schema.compare_load_md5_metadata_tbl(ARRAY['test_mv']);
    SELECT * from pdcd_schema.compare_load_md5_metadata_functions(ARRAY['test_mv']);
    TRUNCATE TABLE pdcd_schema.md5_metadata_staging_tbl RESTART IDENTITY CASCADE;
    TRUNCATE TABLE pdcd_schema.md5_metadata_staging_functions RESTART IDENTITY CASCADE;
    SELECT * FROM pdcd_schema.load_md5_metadata_staging_tbl(ARRAY['test_mv']);
    SELECT * from pdcd_schema.load_md5_metadata_staging_functions(ARRAY['test_mv']);

select metadata_id,snapshot_id,schema_name,object_type,object_type_name,object_subtype,object_subtype_name,object_md5,processed_time,change_type FROM pdcd_schema.md5_metadata_tbl;
          31 |           4 | test_mv     | Materialized View | mv_sales_daily     |                |                         | 6267aad127730a20c5289a8d88395e4b | 2025-11-26 17:43:25.779642 | ADDED
          32 |           4 | test_mv     | Materialized View | mv_sales_daily_v1  |                |                         | 2f41c4950f6966e48ea26de31d9c43aa | 2025-11-26 17:43:25.779677 | DELETED
          33 |           5 | test_mv     | Table             | v_sales_daily_high | Column         | total_amount            | a24ea704b07d3430df42fc104309d38d | 2025-11-26 17:55:55.312114 | MODIFIED
          34 |           5 | test_mv     | Table             | v_sales_daily_high | Column         | txn_cnt                 | 281220c8e13d3604037c4cb48f84b433 | 2025-11-26 17:55:55.321943 | MODIFIED
          35 |           5 | test_mv     | Table             | v_sales_daily_high | Column         | segment                 | ad769f35dcc10b375c867f5b92e6a0ac | 2025-11-26 17:55:55.322023 | DELETED
          36 |           5 | test_mv     | View              | v_sales_daily_high |                |                         | 5d97e69af1bd73f51f798f30677ff9d3 | 2025-11-26 17:55:58.520261 | MODIFIED
          37 |           5 | test_mv     | Materialized View | mv_sales_daily     |                |                         | 2f41c4950f6966e48ea26de31d9c43aa | 2025-11-26 17:55:58.520289 | MODIFIED
(37 rows)

select metadata_id,snapshot_id,schema_name,object_type,object_type_name,object_subtype,object_subtype_name,object_md5,processed_time from pdcd_schema.md5_metadata_staging_tbl;
 metadata_id | snapshot_id | schema_name |    object_type    |  object_type_name  | object_subtype |   object_subtype_name   |            object_md5            |       processed_time
-------------+-------------+-------------+-------------------+--------------------+----------------+-------------------------+----------------------------------+----------------------------
           1 |           5 | test_mv     | Table             | sales_txn          | Column         | cust_id                 | 8aa548f7ce8c4f95f6dc839e757d25c0 | 2025-11-26 17:55:58.571302
           2 |           5 | test_mv     | Table             | sales_txn          | Column         | txn_date                | a8872c6b2153479b58da9f8414912303 | 2025-11-26 17:55:58.575044
           3 |           5 | test_mv     | Table             | customers          | Column         | cust_name               | b537a75e4c2744b85e478bf937370ba9 | 2025-11-26 17:55:58.575058
           4 |           5 | test_mv     | Table             | sales_txn          | Column         | txn_id                  | 85595afcdc233d8b832de65dfc5c9d13 | 2025-11-26 17:55:58.575064
           5 |           5 | test_mv     | Table             | v_sales_daily_high | Column         | txn_cnt                 | 281220c8e13d3604037c4cb48f84b433 | 2025-11-26 17:55:58.575069
           6 |           5 | test_mv     | Table             | customers          | Column         | cust_id                 | e6c2480669939991ff6fe41c5229279b | 2025-11-26 17:55:58.575073
           7 |           5 | test_mv     | Table             | sales_txn          | Column         | amount                  | 92b69c3ff65f71e899dfd03c9f0cb199 | 2025-11-26 17:55:58.575078
           8 |           5 | test_mv     | Table             | v_sales_daily_high | Column         | txn_date                | 1efa9c3fa16d52cb8394cce206ad18a3 | 2025-11-26 17:55:58.575083
           9 |           5 | test_mv     | Table             | customers          | Column         | segment                 | 350fa0710b624e92cd2d3439c54cee88 | 2025-11-26 17:55:58.575088
          10 |           5 | test_mv     | Table             | v_sales_daily_high | Column         | total_amount            | a24ea704b07d3430df42fc104309d38d | 2025-11-26 17:55:58.575093
          11 |           5 | test_mv     | Table             | sales_txn          | Constraint     | sales_txn_pkey          | 6843002dcdbadf4d026ef385f319f49e | 2025-11-26 17:55:58.576852
          12 |           5 | test_mv     | Table             | customers          | Constraint     | customers_pkey          | 0af9a8482197dc79fb7e3c157f2371fd | 2025-11-26 17:55:58.576866
          13 |           5 | test_mv     | Table             | sales_txn          | Constraint     | sales_txn_cust_id_fkey  | 1c65fb937d94cae364bae3cdc959678d | 2025-11-26 17:55:58.576871
          14 |           5 | test_mv     | Table             | sales_txn          | Index          | sales_txn_pkey          | fd723bbd8a95efe60c59f64e9673c6d1 | 2025-11-26 17:55:58.579356
          15 |           5 | test_mv     | Table             | customers          | Index          | customers_pkey          | 732a1593e99980321e70519dac105e49 | 2025-11-26 17:55:58.57937
          16 |           5 | test_mv     | Materialized View | mv_sales_daily     | Index          | idx_mv_sales_daily_date | 4d66be207f0c470f85c2109d3340e23c | 2025-11-26 17:55:58.579375
          17 |           5 | test_mv     | Table             | sales_txn          | Reference      | sales_txn_cust_id_fkey  | c11aeff0e53dc6614383720e22b83ac7 | 2025-11-26 17:55:58.584193
          18 |           5 | test_mv     | Table             | sales_txn          | Sequence       | sales_txn_txn_id_seq    | ed48d9f699661cbdf84b30f41cc02dc3 | 2025-11-26 17:55:58.586688
(18 rows)

select metadata_id,snapshot_id,schema_name,object_type,object_type_name,object_subtype,object_subtype_name,object_subtype_details,object_md5,processed_time from pdcd_schema.md5_metadata_staging_functions;

 metadata_id | snapshot_id | schema_name |    object_type    |  object_type_name  | object_subtype | object_subtype_name |                                                                 object_subtype_details                                                                  |            object_md5            |       processed_time
-------------+-------------+-------------+-------------------+--------------------+----------------+---------------------+---------------------------------------------------------------------------------------------------------------------------------------------------------+----------------------------------+----------------------------
           1 |           5 | test_mv     | View              | v_sales_daily_high |                |                     | view_type:VIEW,view_definition: SELECT mv_sales_daily.txn_date,                                                                                        +| 5d97e69af1bd73f51f798f30677ff9d3 | 2025-11-26 17:55:59.060227
             |             |             |                   |                    |                |                     |     mv_sales_daily.txn_cnt,                                                                                                                            +|                                  |
             |             |             |                   |                    |                |                     |     mv_sales_daily.total_amount                                                                                                                        +|                                  |
             |             |             |                   |                    |                |                     |    FROM test_mv.mv_sales_daily                                                                                                                         +|                                  |
             |             |             |                   |                    |                |                     |   WHERE mv_sales_daily.total_amount > 5000::numeric;,base_tables:,view_owner:test_user,dependent_objects:Dependent views: test_mv.mv_sales_daily        |                                  |
           2 |           5 | test_mv     | Materialized View | mv_sales_daily     |                |                     | view_type:MATERIALIZED VIEW,view_definition: SELECT sales_txn.txn_date,                                                                                +| 2f41c4950f6966e48ea26de31d9c43aa | 2025-11-26 17:55:59.077563
             |             |             |                   |                    |                |                     |     count(*) AS txn_cnt,                                                                                                                               +|                                  |
             |             |             |                   |                    |                |                     |     sum(sales_txn.amount) AS total_amount                                                                                                              +|                                  |
             |             |             |                   |                    |                |                     |    FROM test_mv.sales_txn                                                                                                                              +|                                  |
             |             |             |                   |                    |                |                     |   GROUP BY sales_txn.txn_date;,base_tables:test_mv.sales_txn,is_ populated:true,view_owner:test_user,dependent_objects:Indexes: idx_mv_sales_daily_date |                                  |
(2 rows)


--* Test Run 5 — Index delete / add / change

-- 1. Check existing index
-- \d+ test_mv.mv_sales_daily

-- 2. Drop index
DROP INDEX IF EXISTS test_mv.idx_mv_sales_daily_date;

-- 3. Validate query still works but without that index
-- EXPLAIN ANALYZE
-- SELECT * FROM test_mv.mv_sales_daily WHERE txn_date = CURRENT_DATE - 1;

-- 4. Create a different index (e.g., on total_amount)
CREATE INDEX idx_mv_sales_daily_total
    ON test_mv.mv_sales_daily (total_amount);

-- 5. Validate usage of new index
-- EXPLAIN ANALYZE
-- SELECT * FROM test_mv.mv_sales_daily
--  WHERE total_amount > 5000;

    SELECT * FROM pdcd_schema.load_snapshot_tbl();
    SELECT * FROM pdcd_schema.compare_load_md5_metadata_tbl(ARRAY['test_mv']);
    SELECT * from pdcd_schema.compare_load_md5_metadata_functions(ARRAY['test_mv']);
    TRUNCATE TABLE pdcd_schema.md5_metadata_staging_tbl RESTART IDENTITY CASCADE;
    TRUNCATE TABLE pdcd_schema.md5_metadata_staging_functions RESTART IDENTITY CASCADE;
    SELECT * FROM pdcd_schema.load_md5_metadata_staging_tbl(ARRAY['test_mv']);
    SELECT * from pdcd_schema.load_md5_metadata_staging_functions(ARRAY['test_mv']);

select metadata_id,snapshot_id,schema_name,object_type,object_type_name,object_subtype,object_subtype_name,object_md5,processed_time,change_type FROM pdcd_schema.md5_metadata_tbl;
          37 |           5 | test_mv     | Materialized View | mv_sales_daily     |                |                          | 2f41c4950f6966e48ea26de31d9c43aa | 2025-11-26 17:55:58.520289 | MODIFIED
          38 |           6 | test_mv     | Materialized View | mv_sales_daily     | Index          | idx_mv_sales_daily_total | 773b0a71ff7ca760ba4c3622d3dd848d | 2025-11-26 18:03:26.223159 | ADDED
          39 |           6 | test_mv     | Materialized View | mv_sales_daily     | Index          | idx_mv_sales_daily_date  | 4d66be207f0c470f85c2109d3340e23c | 2025-11-26 18:03:26.223248 | DELETED
          40 |           6 | test_mv     | Materialized View | mv_sales_daily     |                |                          | 0bd7bf01af0d7f76ded7252264fb7984 | 2025-11-26 18:03:28.46459  | MODIFIED
(40 rows)

select metadata_id,snapshot_id,schema_name,object_type,object_type_name,object_subtype,object_subtype_name,object_md5,processed_time from pdcd_schema.md5_metadata_staging_tbl;

 metadata_id | snapshot_id | schema_name |    object_type    |  object_type_name  | object_subtype |   object_subtype_name    |            object_md5            |       processed_time
-------------+-------------+-------------+-------------------+--------------------+----------------+--------------------------+----------------------------------+----------------------------
           1 |           6 | test_mv     | Table             | sales_txn          | Column         | cust_id                  | 8aa548f7ce8c4f95f6dc839e757d25c0 | 2025-11-26 18:03:28.514519
           2 |           6 | test_mv     | Table             | sales_txn          | Column         | txn_date                 | a8872c6b2153479b58da9f8414912303 | 2025-11-26 18:03:28.520587
           3 |           6 | test_mv     | Table             | customers          | Column         | cust_name                | b537a75e4c2744b85e478bf937370ba9 | 2025-11-26 18:03:28.520593
           4 |           6 | test_mv     | Table             | sales_txn          | Column         | txn_id                   | 85595afcdc233d8b832de65dfc5c9d13 | 2025-11-26 18:03:28.520596
           5 |           6 | test_mv     | Table             | v_sales_daily_high | Column         | txn_cnt                  | 281220c8e13d3604037c4cb48f84b433 | 2025-11-26 18:03:28.520599
           6 |           6 | test_mv     | Table             | customers          | Column         | cust_id                  | e6c2480669939991ff6fe41c5229279b | 2025-11-26 18:03:28.520601
           7 |           6 | test_mv     | Table             | sales_txn          | Column         | amount                   | 92b69c3ff65f71e899dfd03c9f0cb199 | 2025-11-26 18:03:28.520604
           8 |           6 | test_mv     | Table             | v_sales_daily_high | Column         | txn_date                 | 1efa9c3fa16d52cb8394cce206ad18a3 | 2025-11-26 18:03:28.520606
           9 |           6 | test_mv     | Table             | customers          | Column         | segment                  | 350fa0710b624e92cd2d3439c54cee88 | 2025-11-26 18:03:28.520609
          10 |           6 | test_mv     | Table             | v_sales_daily_high | Column         | total_amount             | a24ea704b07d3430df42fc104309d38d | 2025-11-26 18:03:28.520611
          11 |           6 | test_mv     | Table             | sales_txn          | Constraint     | sales_txn_pkey           | 6843002dcdbadf4d026ef385f319f49e | 2025-11-26 18:03:28.522336
          12 |           6 | test_mv     | Table             | customers          | Constraint     | customers_pkey           | 0af9a8482197dc79fb7e3c157f2371fd | 2025-11-26 18:03:28.522347
          13 |           6 | test_mv     | Table             | sales_txn          | Constraint     | sales_txn_cust_id_fkey   | 1c65fb937d94cae364bae3cdc959678d | 2025-11-26 18:03:28.522352
          14 |           6 | test_mv     | Table             | sales_txn          | Index          | sales_txn_pkey           | fd723bbd8a95efe60c59f64e9673c6d1 | 2025-11-26 18:03:28.525347
          15 |           6 | test_mv     | Table             | customers          | Index          | customers_pkey           | 732a1593e99980321e70519dac105e49 | 2025-11-26 18:03:28.525358
          16 |           6 | test_mv     | Materialized View | mv_sales_daily     | Index          | idx_mv_sales_daily_total | 773b0a71ff7ca760ba4c3622d3dd848d | 2025-11-26 18:03:28.525364
          17 |           6 | test_mv     | Table             | sales_txn          | Reference      | sales_txn_cust_id_fkey   | c11aeff0e53dc6614383720e22b83ac7 | 2025-11-26 18:03:28.530125
          18 |           6 | test_mv     | Table             | sales_txn          | Sequence       | sales_txn_txn_id_seq     | ed48d9f699661cbdf84b30f41cc02dc3 | 2025-11-26 18:03:28.532309
(18 rows)


select metadata_id,snapshot_id,schema_name,object_type,object_type_name,object_subtype,object_subtype_name,object_subtype_details,object_md5,processed_time from pdcd_schema.md5_metadata_staging_functions;
 metadata_id | snapshot_id | schema_name |    object_type    |  object_type_name  | object_subtype | object_subtype_name |                                                                  object_subtype_details                                                                  |            object_md5            |       processed_time
-------------+-------------+-------------+-------------------+--------------------+----------------+---------------------+----------------------------------------------------------------------------------------------------------------------------------------------------------+----------------------------------+----------------------------
           1 |           6 | test_mv     | View              | v_sales_daily_high |                |                     | view_type:VIEW,view_definition: SELECT mv_sales_daily.txn_date,                                                                                         +| 5d97e69af1bd73f51f798f30677ff9d3 | 2025-11-26 18:03:28.754707
             |             |             |                   |                    |                |                     |     mv_sales_daily.txn_cnt,                                                                                                                             +|                                  |
             |             |             |                   |                    |                |                     |     mv_sales_daily.total_amount                                                                                                                         +|                                  |
             |             |             |                   |                    |                |                     |    FROM test_mv.mv_sales_daily                                                                                                                          +|                                  |
             |             |             |                   |                    |                |                     |   WHERE mv_sales_daily.total_amount > 5000::numeric;,base_tables:,view_owner:test_user,dependent_objects:Dependent views: test_mv.mv_sales_daily         |                                  |
           2 |           6 | test_mv     | Materialized View | mv_sales_daily     |                |                     | view_type:MATERIALIZED VIEW,view_definition: SELECT sales_txn.txn_date,                                                                                 +| 0bd7bf01af0d7f76ded7252264fb7984 | 2025-11-26 18:03:28.772488
             |             |             |                   |                    |                |                     |     count(*) AS txn_cnt,                                                                                                                                +|                                  |
             |             |             |                   |                    |                |                     |     sum(sales_txn.amount) AS total_amount                                                                                                               +|                                  |
             |             |             |                   |                    |                |                     |    FROM test_mv.sales_txn                                                                                                                               +|                                  |
             |             |             |                   |                    |                |                     |   GROUP BY sales_txn.txn_date;,base_tables:test_mv.sales_txn,is_ populated:true,view_owner:test_user,dependent_objects:Indexes: idx_mv_sales_daily_total |                                  |
(2 rows)


--* Test Run 6 — Add Another Dependent View (hierarchy of dependencies)

-- 1. Existing base dependent view already there: v_sales_daily_high

-- 2. Create a second dependent view on top of first one
CREATE VIEW test_mv.v_sales_daily_gold_high AS
SELECT
    d.txn_date,
    d.total_amount
FROM test_mv.v_sales_daily_high d
JOIN test_mv.customers c
  ON c.cust_id IN (
      SELECT cust_id
      FROM test_mv.sales_txn s
      WHERE s.txn_date = d.txn_date
  )
WHERE c.segment = 'GOLD';

-- 3. Validation
-- SELECT * FROM test_mv.v_sales_daily_gold_high ORDER BY txn_date;

    SELECT * FROM pdcd_schema.load_snapshot_tbl();
    SELECT * FROM pdcd_schema.compare_load_md5_metadata_tbl(ARRAY['test_mv']);
    SELECT * from pdcd_schema.compare_load_md5_metadata_functions(ARRAY['test_mv']);
    TRUNCATE TABLE pdcd_schema.md5_metadata_staging_tbl RESTART IDENTITY CASCADE;
    TRUNCATE TABLE pdcd_schema.md5_metadata_staging_functions RESTART IDENTITY CASCADE;
    SELECT * FROM pdcd_schema.load_md5_metadata_staging_tbl(ARRAY['test_mv']);
    SELECT * from pdcd_schema.load_md5_metadata_staging_functions(ARRAY['test_mv']);

select metadata_id,snapshot_id,schema_name,object_type,object_type_name,object_subtype,object_subtype_name,object_md5,processed_time,change_type FROM pdcd_schema.md5_metadata_tbl;
          39 |           6 | test_mv     | Materialized View | mv_sales_daily          | Index          | idx_mv_sales_daily_date  | 4d66be207f0c470f85c2109d3340e23c | 2025-11-26 18:03:26.223248 | DELETED
          40 |           6 | test_mv     | Materialized View | mv_sales_daily          |                |                          | 0bd7bf01af0d7f76ded7252264fb7984 | 2025-11-26 18:03:28.46459  | MODIFIED
          41 |           7 | test_mv     | Table             | v_sales_daily_gold_high | Column         | txn_date                 | 1efa9c3fa16d52cb8394cce206ad18a3 | 2025-11-26 18:07:09.210379 | ADDED
          42 |           7 | test_mv     | Table             | v_sales_daily_gold_high | Column         | total_amount             | 8eca8756e1ba7d796bdd2e700d366d80 | 2025-11-26 18:07:09.210428 | ADDED
          43 |           7 | test_mv     | View              | v_sales_daily_gold_high |                |                          | b6d3aa143c4680f2a5a5342feca40d1b | 2025-11-26 18:07:10.342377 | ADDED
(43 rows)

select metadata_id,snapshot_id,schema_name,object_type,object_type_name,object_subtype,object_subtype_name,object_md5,processed_time from pdcd_schema.md5_metadata_staging_tbl;

 metadata_id | snapshot_id | schema_name |    object_type    |    object_type_name     | object_subtype |   object_subtype_name    |            object_md5            |       processed_time
-------------+-------------+-------------+-------------------+-------------------------+----------------+--------------------------+----------------------------------+----------------------------
           1 |           7 | test_mv     | Table             | sales_txn               | Column         | cust_id                  | 8aa548f7ce8c4f95f6dc839e757d25c0 | 2025-11-26 18:07:10.395495
           2 |           7 | test_mv     | Table             | sales_txn               | Column         | txn_date                 | a8872c6b2153479b58da9f8414912303 | 2025-11-26 18:07:10.401921
           3 |           7 | test_mv     | Table             | customers               | Column         | cust_name                | b537a75e4c2744b85e478bf937370ba9 | 2025-11-26 18:07:10.401932
           4 |           7 | test_mv     | Table             | sales_txn               | Column         | txn_id                   | 85595afcdc233d8b832de65dfc5c9d13 | 2025-11-26 18:07:10.401938
           5 |           7 | test_mv     | Table             | v_sales_daily_high      | Column         | txn_cnt                  | 281220c8e13d3604037c4cb48f84b433 | 2025-11-26 18:07:10.401943
           6 |           7 | test_mv     | Table             | customers               | Column         | cust_id                  | e6c2480669939991ff6fe41c5229279b | 2025-11-26 18:07:10.401948
           7 |           7 | test_mv     | Table             | sales_txn               | Column         | amount                   | 92b69c3ff65f71e899dfd03c9f0cb199 | 2025-11-26 18:07:10.401953
           8 |           7 | test_mv     | Table             | v_sales_daily_gold_high | Column         | txn_date                 | 1efa9c3fa16d52cb8394cce206ad18a3 | 2025-11-26 18:07:10.401958
           9 |           7 | test_mv     | Table             | v_sales_daily_high      | Column         | txn_date                 | 1efa9c3fa16d52cb8394cce206ad18a3 | 2025-11-26 18:07:10.401962
          10 |           7 | test_mv     | Table             | customers               | Column         | segment                  | 350fa0710b624e92cd2d3439c54cee88 | 2025-11-26 18:07:10.401968
          11 |           7 | test_mv     | Table             | v_sales_daily_high      | Column         | total_amount             | a24ea704b07d3430df42fc104309d38d | 2025-11-26 18:07:10.401976
          12 |           7 | test_mv     | Table             | v_sales_daily_gold_high | Column         | total_amount             | 8eca8756e1ba7d796bdd2e700d366d80 | 2025-11-26 18:07:10.401981
          13 |           7 | test_mv     | Table             | sales_txn               | Constraint     | sales_txn_pkey           | 6843002dcdbadf4d026ef385f319f49e | 2025-11-26 18:07:10.403994
          14 |           7 | test_mv     | Table             | customers               | Constraint     | customers_pkey           | 0af9a8482197dc79fb7e3c157f2371fd | 2025-11-26 18:07:10.404005
          15 |           7 | test_mv     | Table             | sales_txn               | Constraint     | sales_txn_cust_id_fkey   | 1c65fb937d94cae364bae3cdc959678d | 2025-11-26 18:07:10.40401
          16 |           7 | test_mv     | Table             | sales_txn               | Index          | sales_txn_pkey           | fd723bbd8a95efe60c59f64e9673c6d1 | 2025-11-26 18:07:10.406508
          17 |           7 | test_mv     | Table             | customers               | Index          | customers_pkey           | 732a1593e99980321e70519dac105e49 | 2025-11-26 18:07:10.406516
          18 |           7 | test_mv     | Materialized View | mv_sales_daily          | Index          | idx_mv_sales_daily_total | 773b0a71ff7ca760ba4c3622d3dd848d | 2025-11-26 18:07:10.406521
          19 |           7 | test_mv     | Table             | sales_txn               | Reference      | sales_txn_cust_id_fkey   | c11aeff0e53dc6614383720e22b83ac7 | 2025-11-26 18:07:10.410858
          20 |           7 | test_mv     | Table             | sales_txn               | Sequence       | sales_txn_txn_id_seq     | ed48d9f699661cbdf84b30f41cc02dc3 | 2025-11-26 18:07:10.412809
(20 rows)

select metadata_id,snapshot_id,schema_name,object_type,object_type_name,object_subtype,object_subtype_name,object_subtype_details,object_md5,processed_time from pdcd_schema.md5_metadata_staging_functions;

 metadata_id | snapshot_id | schema_name |    object_type    |    object_type_name     | object_subtype | object_subtype_name |                                                                           object_subtype_details                                                                            |            object_md5            |       processed_time
-------------+-------------+-------------+-------------------+-------------------------+----------------+---------------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------+----------------------------------+----------------------------
           1 |           7 | test_mv     | View              | v_sales_daily_high      |                |                     | view_type:VIEW,view_definition: SELECT mv_sales_daily.txn_date,                                                                                                            +| 5d97e69af1bd73f51f798f30677ff9d3 | 2025-11-26 18:07:11.128668
             |             |             |                   |                         |                |                     |     mv_sales_daily.txn_cnt,                                                                                                                                                +|                                  |
             |             |             |                   |                         |                |                     |     mv_sales_daily.total_amount                                                                                                                                            +|                                  |
             |             |             |                   |                         |                |                     |    FROM test_mv.mv_sales_daily                                                                                                                                             +|                                  |
             |             |             |                   |                         |                |                     |   WHERE mv_sales_daily.total_amount > 5000::numeric;,base_tables:,view_owner:test_user,dependent_objects:Dependent views: test_mv.mv_sales_daily                            |                                  |
           2 |           7 | test_mv     | View              | v_sales_daily_gold_high |                |                     | view_type:VIEW,view_definition: SELECT d.txn_date,                                                                                                                         +| b6d3aa143c4680f2a5a5342feca40d1b | 2025-11-26 18:07:11.136645
             |             |             |                   |                         |                |                     |     d.total_amount                                                                                                                                                         +|                                  |
             |             |             |                   |                         |                |                     |    FROM test_mv.v_sales_daily_high d                                                                                                                                       +|                                  |
             |             |             |                   |                         |                |                     |      JOIN test_mv.customers c ON (c.cust_id IN ( SELECT s.cust_id                                                                                                          +|                                  |
             |             |             |                   |                         |                |                     |            FROM test_mv.sales_txn s                                                                                                                                        +|                                  |
             |             |             |                   |                         |                |                     |           WHERE s.txn_date = d.txn_date))                                                                                                                                  +|                                  |
             |             |             |                   |                         |                |                     |   WHERE c.segment::text = 'GOLD'::text;,base_tables:test_mv.customers, test_mv.sales_txn,view_owner:test_user,dependent_objects:Dependent views: test_mv.v_sales_daily_high |                                  |
           3 |           7 | test_mv     | Materialized View | mv_sales_daily          |                |                     | view_type:MATERIALIZED VIEW,view_definition: SELECT sales_txn.txn_date,                                                                                                    +| 0bd7bf01af0d7f76ded7252264fb7984 | 2025-11-26 18:07:11.147505
             |             |             |                   |                         |                |                     |     count(*) AS txn_cnt,                                                                                                                                                   +|                                  |
             |             |             |                   |                         |                |                     |     sum(sales_txn.amount) AS total_amount                                                                                                                                  +|                                  |
             |             |             |                   |                         |                |                     |    FROM test_mv.sales_txn                                                                                                                                                  +|                                  |
             |             |             |                   |                         |                |                     |   GROUP BY sales_txn.txn_date;,base_tables:test_mv.sales_txn,is_ populated:true,view_owner:test_user,dependent_objects:Indexes: idx_mv_sales_daily_total                    |                                  |
(3 rows)

--* Test case 7 - Modify MV Again with Full Dependency Impact

-- 1. Create a new MV version with more detail
CREATE MATERIALIZED VIEW test_mv.mv_sales_daily_v3 AS
SELECT
    s.txn_date,
    s.cust_id,
    COUNT(*)        AS txn_cnt,
    SUM(s.amount)   AS total_amount
FROM test_mv.sales_txn s
GROUP BY s.txn_date, s.cust_id;

-- 2. Update direct dependent view to use v3
CREATE OR REPLACE VIEW test_mv.v_sales_daily_high AS
SELECT
    txn_date,
    cust_id,
    txn_cnt,
    total_amount
FROM test_mv.mv_sales_daily_v3
WHERE total_amount > 5000;

-- 3. Update second-level dependent view
CREATE OR REPLACE VIEW test_mv.v_sales_daily_gold_high AS
SELECT
    h.txn_date,
    h.cust_id,
    h.total_amount
FROM test_mv.v_sales_daily_high h
JOIN test_mv.customers c ON c.cust_id = h.cust_id
WHERE c.segment = 'GOLD';

-- 4. Drop old mv_sales_daily and rename v3
DROP MATERIALIZED VIEW test_mv.mv_sales_daily CASCADE;  -- drops old deps
ALTER MATERIALIZED VIEW test_mv.mv_sales_daily_v3
    RENAME TO mv_sales_daily;

-- 5. Recreate views on top of new canonical MV
CREATE OR REPLACE VIEW test_mv.v_sales_daily_high AS
SELECT
    txn_date,
    cust_id,
    txn_cnt,
    total_amount
FROM test_mv.mv_sales_daily
WHERE total_amount > 5000;

CREATE OR REPLACE VIEW test_mv.v_sales_daily_gold_high AS
SELECT
    h.txn_date,
    h.cust_id,
    h.total_amount
FROM test_mv.v_sales_daily_high h
JOIN test_mv.customers c ON c.cust_id = h.cust_id
WHERE c.segment = 'GOLD';

-- 6. Rebuild index on the new MV
CREATE INDEX idx_mv_sales_daily_date
    ON test_mv.mv_sales_daily (txn_date);

-- 7. Validation
-- SELECT * FROM test_mv.mv_sales_daily ORDER BY txn_date, cust_id;
-- SELECT * FROM test_mv.v_sales_daily_high ORDER BY txn_date, cust_id;
-- SELECT * FROM test_mv.v_sales_daily_gold_high ORDER BY txn_date, cust_id;

    SELECT * FROM pdcd_schema.load_snapshot_tbl();
    SELECT * FROM pdcd_schema.compare_load_md5_metadata_tbl(ARRAY['test_mv']);
    SELECT * from pdcd_schema.compare_load_md5_metadata_functions(ARRAY['test_mv']);
    TRUNCATE TABLE pdcd_schema.md5_metadata_staging_tbl RESTART IDENTITY CASCADE;
    TRUNCATE TABLE pdcd_schema.md5_metadata_staging_functions RESTART IDENTITY CASCADE;
    SELECT * FROM pdcd_schema.load_md5_metadata_staging_tbl(ARRAY['test_mv']);
    SELECT * from pdcd_schema.load_md5_metadata_staging_functions(ARRAY['test_mv']);

select metadata_id,snapshot_id,schema_name,object_type,object_type_name,object_subtype,object_subtype_name,object_md5,processed_time,change_type FROM pdcd_schema.md5_metadata_tbl;
          44 |           8 | test_mv     | Table             | v_sales_daily_gold_high | Column         | total_amount             | a24ea704b07d3430df42fc104309d38d | 2025-11-26 18:14:01.210185 | MODIFIED
          45 |           8 | test_mv     | Table             | v_sales_daily_high      | Column         | total_amount             | 7fbb864c17b98b7a484d262e78fffe6e | 2025-11-26 18:14:01.210639 | MODIFIED
          46 |           8 | test_mv     | Table             | v_sales_daily_high      | Column         | txn_cnt                  | 1d53716ad4462a6e22b711ad4e6feb79 | 2025-11-26 18:14:01.21065  | MODIFIED
          47 |           8 | test_mv     | Table             | v_sales_daily_gold_high | Column         | cust_id                  | c1bd38510071b86246e457243c970262 | 2025-11-26 18:14:01.210694 | ADDED
          48 |           8 | test_mv     | Table             | v_sales_daily_high      | Column         | cust_id                  | c1bd38510071b86246e457243c970262 | 2025-11-26 18:14:01.210701 | ADDED
          49 |           8 | test_mv     | Materialized View | mv_sales_daily          | Index          | idx_mv_sales_daily_date  | 4d66be207f0c470f85c2109d3340e23c | 2025-11-26 18:14:01.210709 | ADDED
          50 |           8 | test_mv     | Materialized View | mv_sales_daily          | Index          | idx_mv_sales_daily_total | 773b0a71ff7ca760ba4c3622d3dd848d | 2025-11-26 18:14:01.210752 | DELETED
          51 |           8 | test_mv     | View              | v_sales_daily_gold_high |                |                          | 6d6d23f8efcdadf3c56c238d46ff0703 | 2025-11-26 18:14:03.475945 | MODIFIED
          52 |           8 | test_mv     | View              | v_sales_daily_high      |                |                          | b7aec55c97b0d53f4d7c131eb6ef2574 | 2025-11-26 18:14:03.47601  | MODIFIED
          53 |           8 | test_mv     | Materialized View | mv_sales_daily          |                |                          | df7187de34addb5f56e86c4410ca98aa | 2025-11-26 18:14:03.47603  | MODIFIED
(53 rows)

select metadata_id,snapshot_id,schema_name,object_type,object_type_name,object_subtype,object_subtype_name,object_md5,processed_time from pdcd_schema.md5_metadata_staging_tbl;

 metadata_id | snapshot_id | schema_name |    object_type    |    object_type_name     | object_subtype |   object_subtype_name   |            object_md5            |       processed_time
-------------+-------------+-------------+-------------------+-------------------------+----------------+-------------------------+----------------------------------+----------------------------
           1 |           8 | test_mv     | Table             | sales_txn               | Column         | cust_id                 | 8aa548f7ce8c4f95f6dc839e757d25c0 | 2025-11-26 18:14:03.525184
           2 |           8 | test_mv     | Table             | v_sales_daily_gold_high | Column         | total_amount            | a24ea704b07d3430df42fc104309d38d | 2025-11-26 18:14:03.532413
           3 |           8 | test_mv     | Table             | sales_txn               | Column         | txn_date                | a8872c6b2153479b58da9f8414912303 | 2025-11-26 18:14:03.532438
           4 |           8 | test_mv     | Table             | customers               | Column         | cust_name               | b537a75e4c2744b85e478bf937370ba9 | 2025-11-26 18:14:03.532447
           5 |           8 | test_mv     | Table             | sales_txn               | Column         | txn_id                  | 85595afcdc233d8b832de65dfc5c9d13 | 2025-11-26 18:14:03.53246
           6 |           8 | test_mv     | Table             | customers               | Column         | cust_id                 | e6c2480669939991ff6fe41c5229279b | 2025-11-26 18:14:03.532468
           7 |           8 | test_mv     | Table             | sales_txn               | Column         | amount                  | 92b69c3ff65f71e899dfd03c9f0cb199 | 2025-11-26 18:14:03.532475
           8 |           8 | test_mv     | Table             | v_sales_daily_gold_high | Column         | txn_date                | 1efa9c3fa16d52cb8394cce206ad18a3 | 2025-11-26 18:14:03.532486
           9 |           8 | test_mv     | Table             | v_sales_daily_high      | Column         | cust_id                 | c1bd38510071b86246e457243c970262 | 2025-11-26 18:14:03.532493
          10 |           8 | test_mv     | Table             | v_sales_daily_high      | Column         | txn_date                | 1efa9c3fa16d52cb8394cce206ad18a3 | 2025-11-26 18:14:03.532498
          11 |           8 | test_mv     | Table             | customers               | Column         | segment                 | 350fa0710b624e92cd2d3439c54cee88 | 2025-11-26 18:14:03.532506
          12 |           8 | test_mv     | Table             | v_sales_daily_high      | Column         | txn_cnt                 | 1d53716ad4462a6e22b711ad4e6feb79 | 2025-11-26 18:14:03.532514
          13 |           8 | test_mv     | Table             | v_sales_daily_high      | Column         | total_amount            | 7fbb864c17b98b7a484d262e78fffe6e | 2025-11-26 18:14:03.53252
          14 |           8 | test_mv     | Table             | v_sales_daily_gold_high | Column         | cust_id                 | c1bd38510071b86246e457243c970262 | 2025-11-26 18:14:03.532533
          15 |           8 | test_mv     | Table             | sales_txn               | Constraint     | sales_txn_pkey          | 6843002dcdbadf4d026ef385f319f49e | 2025-11-26 18:14:03.53507
          16 |           8 | test_mv     | Table             | customers               | Constraint     | customers_pkey          | 0af9a8482197dc79fb7e3c157f2371fd | 2025-11-26 18:14:03.535091
          17 |           8 | test_mv     | Table             | sales_txn               | Constraint     | sales_txn_cust_id_fkey  | 1c65fb937d94cae364bae3cdc959678d | 2025-11-26 18:14:03.535097
          18 |           8 | test_mv     | Table             | sales_txn               | Index          | sales_txn_pkey          | fd723bbd8a95efe60c59f64e9673c6d1 | 2025-11-26 18:14:03.53853
          19 |           8 | test_mv     | Table             | customers               | Index          | customers_pkey          | 732a1593e99980321e70519dac105e49 | 2025-11-26 18:14:03.538545
          20 |           8 | test_mv     | Materialized View | mv_sales_daily          | Index          | idx_mv_sales_daily_date | 4d66be207f0c470f85c2109d3340e23c | 2025-11-26 18:14:03.538548
          21 |           8 | test_mv     | Table             | sales_txn               | Reference      | sales_txn_cust_id_fkey  | c11aeff0e53dc6614383720e22b83ac7 | 2025-11-26 18:14:03.543512
          22 |           8 | test_mv     | Table             | sales_txn               | Sequence       | sales_txn_txn_id_seq    | ed48d9f699661cbdf84b30f41cc02dc3 | 2025-11-26 18:14:03.545367
(22 rows)

select metadata_id,snapshot_id,schema_name,object_type,object_type_name,object_subtype,object_subtype_name,object_subtype_details,object_md5,processed_time from pdcd_schema.md5_metadata_staging_functions;

 metadata_id | snapshot_id | schema_name |    object_type    |    object_type_name     | object_subtype | object_subtype_name |                                                                   object_subtype_details                                                                   |            object_md5            |       processed_time
-------------+-------------+-------------+-------------------+-------------------------+----------------+---------------------+------------------------------------------------------------------------------------------------------------------------------------------------------------+----------------------------------+----------------------------
           1 |           8 | test_mv     | View              | v_sales_daily_gold_high |                |                     | view_type:VIEW,view_definition: SELECT h.txn_date,                                                                                                        +| 6d6d23f8efcdadf3c56c238d46ff0703 | 2025-11-26 18:14:04.151879
             |             |             |                   |                         |                |                     |     h.cust_id,                                                                                                                                            +|                                  |
             |             |             |                   |                         |                |                     |     h.total_amount                                                                                                                                        +|                                  |
             |             |             |                   |                         |                |                     |    FROM test_mv.v_sales_daily_high h                                                                                                                      +|                                  |
             |             |             |                   |                         |                |                     |      JOIN test_mv.customers c ON c.cust_id = h.cust_id                                                                                                    +|                                  |
             |             |             |                   |                         |                |                     |   WHERE c.segment::text = 'GOLD'::text;,base_tables:test_mv.customers,view_owner:test_user,dependent_objects:Dependent views: test_mv.v_sales_daily_high   |                                  |
           2 |           8 | test_mv     | View              | v_sales_daily_high      |                |                     | view_type:VIEW,view_definition: SELECT mv_sales_daily.txn_date,                                                                                           +| b7aec55c97b0d53f4d7c131eb6ef2574 | 2025-11-26 18:14:04.157102
             |             |             |                   |                         |                |                     |     mv_sales_daily.cust_id,                                                                                                                               +|                                  |
             |             |             |                   |                         |                |                     |     mv_sales_daily.txn_cnt,                                                                                                                               +|                                  |
             |             |             |                   |                         |                |                     |     mv_sales_daily.total_amount                                                                                                                           +|                                  |
             |             |             |                   |                         |                |                     |    FROM test_mv.mv_sales_daily                                                                                                                            +|                                  |
             |             |             |                   |                         |                |                     |   WHERE mv_sales_daily.total_amount > 5000::numeric;,base_tables:,view_owner:test_user,dependent_objects:Dependent views: test_mv.mv_sales_daily           |                                  |
           3 |           8 | test_mv     | Materialized View | mv_sales_daily          |                |                     | view_type:MATERIALIZED VIEW,view_definition: SELECT s.txn_date,                                                                                           +| df7187de34addb5f56e86c4410ca98aa | 2025-11-26 18:14:04.165567
             |             |             |                   |                         |                |                     |     s.cust_id,                                                                                                                                            +|                                  |
             |             |             |                   |                         |                |                     |     count(*) AS txn_cnt,                                                                                                                                  +|                                  |
             |             |             |                   |                         |                |                     |     sum(s.amount) AS total_amount                                                                                                                         +|                                  |
             |             |             |                   |                         |                |                     |    FROM test_mv.sales_txn s                                                                                                                               +|                                  |
             |             |             |                   |                         |                |                     |   GROUP BY s.txn_date, s.cust_id;,base_tables:test_mv.sales_txn,is_ populated:true,view_owner:test_user,dependent_objects:Indexes: idx_mv_sales_daily_date |                                  |
(3 rows)


--* Test Case 8 - Final Mixed Change (Add + Rename + Drop + Modify)

-- 1. Add another MV that aggregates by segment
CREATE MATERIALIZED VIEW test_mv.mv_sales_segment AS
SELECT
    c.segment,
    COUNT(*)        AS txn_cnt,
    SUM(s.amount)   AS total_amount
FROM test_mv.sales_txn s
JOIN test_mv.customers c ON c.cust_id = s.cust_id
GROUP BY c.segment;

-- 2. Create dependent view on this new MV
CREATE VIEW test_mv.v_sales_segment_gold AS
SELECT *
FROM test_mv.mv_sales_segment
WHERE segment = 'GOLD';

-- 3. Rename mv_sales_segment to mv_sales_segment_v1
ALTER MATERIALIZED VIEW test_mv.mv_sales_segment
    RENAME TO mv_sales_segment_v1;

-- 4. Modify definition via new MV & swap
CREATE MATERIALIZED VIEW test_mv.mv_sales_segment_v2 AS
SELECT
    c.segment,
    COUNT(*)        AS txn_cnt,
    AVG(s.amount)   AS avg_amount,
    SUM(s.amount)   AS total_amount
FROM test_mv.sales_txn s
JOIN test_mv.customers c ON c.cust_id = s.cust_id
GROUP BY c.segment;

-- 5. Redirect dependent view to V2
CREATE OR REPLACE VIEW test_mv.v_sales_segment_gold AS
SELECT *
FROM test_mv.mv_sales_segment_v2
WHERE segment = 'GOLD';

-- 6. Drop old v1 MV
DROP MATERIALIZED VIEW test_mv.mv_sales_segment_v1;

-- 7. Rename v2 to canonical name and create index
ALTER MATERIALIZED VIEW test_mv.mv_sales_segment_v2
    RENAME TO mv_sales_segment;

CREATE INDEX idx_mv_sales_segment_segment
    ON test_mv.mv_sales_segment (segment);

-- 8. Validation
-- SELECT * FROM test_mv.mv_sales_segment ORDER BY segment;
-- SELECT * FROM test_mv.v_sales_segment_gold;

    SELECT * FROM pdcd_schema.load_snapshot_tbl();
    SELECT * FROM pdcd_schema.compare_load_md5_metadata_tbl(ARRAY['test_mv']);
    SELECT * from pdcd_schema.compare_load_md5_metadata_functions(ARRAY['test_mv']);
    TRUNCATE TABLE pdcd_schema.md5_metadata_staging_tbl RESTART IDENTITY CASCADE;
    TRUNCATE TABLE pdcd_schema.md5_metadata_staging_functions RESTART IDENTITY CASCADE;
    SELECT * FROM pdcd_schema.load_md5_metadata_staging_tbl(ARRAY['test_mv']);
    SELECT * from pdcd_schema.load_md5_metadata_staging_functions(ARRAY['test_mv']);

select metadata_id,snapshot_id,schema_name,object_type,object_type_name,object_subtype,object_subtype_name,object_md5,processed_time,change_type FROM pdcd_schema.md5_metadata_tbl;
          53 |           8 | test_mv     | Materialized View | mv_sales_daily          |                |                              | df7187de34addb5f56e86c4410ca98aa | 2025-11-26 18:14:03.47603  | MODIFIED
          54 |           9 | test_mv     | Table             | v_sales_segment_gold    | Column         | segment                      | 611bc77470e92422e2a3925563fc9700 | 2025-11-26 18:19:33.787431 | ADDED
          55 |           9 | test_mv     | Table             | v_sales_segment_gold    | Column         | txn_cnt                      | 281220c8e13d3604037c4cb48f84b433 | 2025-11-26 18:19:33.787474 | ADDED
          56 |           9 | test_mv     | Table             | v_sales_segment_gold    | Column         | avg_amount                   | a24ea704b07d3430df42fc104309d38d | 2025-11-26 18:19:33.787481 | ADDED
          57 |           9 | test_mv     | Table             | v_sales_segment_gold    | Column         | total_amount                 | 7fbb864c17b98b7a484d262e78fffe6e | 2025-11-26 18:19:33.787486 | ADDED
          58 |           9 | test_mv     | Materialized View | mv_sales_segment        | Index          | idx_mv_sales_segment_segment | 83bfbe50f931980cd5802d5b4c097b95 | 2025-11-26 18:19:33.787494 | ADDED
          59 |           9 | test_mv     | View              | v_sales_segment_gold    |                |                              | d99b4025e9d1d2088e995747ff892e9b | 2025-11-26 18:19:35.720914 | ADDED
          60 |           9 | test_mv     | Materialized View | mv_sales_segment        |                |                              | d4142002af707c5ac59d459f6c3966e5 | 2025-11-26 18:19:35.720947 | ADDED
(60 rows)

select metadata_id,snapshot_id,schema_name,object_type,object_type_name,object_subtype,object_subtype_name,object_md5,processed_time from pdcd_schema.md5_metadata_staging_tbl;
 metadata_id | snapshot_id | schema_name |    object_type    |    object_type_name     | object_subtype |     object_subtype_name      |            object_md5            |       processed_time
-------------+-------------+-------------+-------------------+-------------------------+----------------+------------------------------+----------------------------------+----------------------------
           1 |           9 | test_mv     | Table             | v_sales_segment_gold    | Column         | segment                      | 611bc77470e92422e2a3925563fc9700 | 2025-11-26 18:19:35.763447
           2 |           9 | test_mv     | Table             | sales_txn               | Column         | cust_id                      | 8aa548f7ce8c4f95f6dc839e757d25c0 | 2025-11-26 18:19:35.769344
           3 |           9 | test_mv     | Table             | v_sales_segment_gold    | Column         | total_amount                 | 7fbb864c17b98b7a484d262e78fffe6e | 2025-11-26 18:19:35.76935
           4 |           9 | test_mv     | Table             | v_sales_daily_gold_high | Column         | total_amount                 | a24ea704b07d3430df42fc104309d38d | 2025-11-26 18:19:35.769352
           5 |           9 | test_mv     | Table             | sales_txn               | Column         | txn_date                     | a8872c6b2153479b58da9f8414912303 | 2025-11-26 18:19:35.769355
           6 |           9 | test_mv     | Table             | v_sales_segment_gold    | Column         | avg_amount                   | a24ea704b07d3430df42fc104309d38d | 2025-11-26 18:19:35.769363
           7 |           9 | test_mv     | Table             | customers               | Column         | cust_name                    | b537a75e4c2744b85e478bf937370ba9 | 2025-11-26 18:19:35.769365
           8 |           9 | test_mv     | Table             | v_sales_segment_gold    | Column         | txn_cnt                      | 281220c8e13d3604037c4cb48f84b433 | 2025-11-26 18:19:35.769367
           9 |           9 | test_mv     | Table             | sales_txn               | Column         | txn_id                       | 85595afcdc233d8b832de65dfc5c9d13 | 2025-11-26 18:19:35.769369
          10 |           9 | test_mv     | Table             | customers               | Column         | cust_id                      | e6c2480669939991ff6fe41c5229279b | 2025-11-26 18:19:35.769371
          11 |           9 | test_mv     | Table             | sales_txn               | Column         | amount                       | 92b69c3ff65f71e899dfd03c9f0cb199 | 2025-11-26 18:19:35.769375
          12 |           9 | test_mv     | Table             | v_sales_daily_gold_high | Column         | txn_date                     | 1efa9c3fa16d52cb8394cce206ad18a3 | 2025-11-26 18:19:35.769377
          13 |           9 | test_mv     | Table             | v_sales_daily_high      | Column         | cust_id                      | c1bd38510071b86246e457243c970262 | 2025-11-26 18:19:35.769379
          14 |           9 | test_mv     | Table             | v_sales_daily_high      | Column         | txn_date                     | 1efa9c3fa16d52cb8394cce206ad18a3 | 2025-11-26 18:19:35.769382
          15 |           9 | test_mv     | Table             | customers               | Column         | segment                      | 350fa0710b624e92cd2d3439c54cee88 | 2025-11-26 18:19:35.769384
          16 |           9 | test_mv     | Table             | v_sales_daily_high      | Column         | txn_cnt                      | 1d53716ad4462a6e22b711ad4e6feb79 | 2025-11-26 18:19:35.769386
          17 |           9 | test_mv     | Table             | v_sales_daily_high      | Column         | total_amount                 | 7fbb864c17b98b7a484d262e78fffe6e | 2025-11-26 18:19:35.769388
          18 |           9 | test_mv     | Table             | v_sales_daily_gold_high | Column         | cust_id                      | c1bd38510071b86246e457243c970262 | 2025-11-26 18:19:35.769391
          19 |           9 | test_mv     | Table             | sales_txn               | Constraint     | sales_txn_pkey               | 6843002dcdbadf4d026ef385f319f49e | 2025-11-26 18:19:35.770808
          20 |           9 | test_mv     | Table             | customers               | Constraint     | customers_pkey               | 0af9a8482197dc79fb7e3c157f2371fd | 2025-11-26 18:19:35.770818
          21 |           9 | test_mv     | Table             | sales_txn               | Constraint     | sales_txn_cust_id_fkey       | 1c65fb937d94cae364bae3cdc959678d | 2025-11-26 18:19:35.770823
          22 |           9 | test_mv     | Materialized View | mv_sales_segment        | Index          | idx_mv_sales_segment_segment | 83bfbe50f931980cd5802d5b4c097b95 | 2025-11-26 18:19:35.773835
          23 |           9 | test_mv     | Table             | sales_txn               | Index          | sales_txn_pkey               | fd723bbd8a95efe60c59f64e9673c6d1 | 2025-11-26 18:19:35.773851
          24 |           9 | test_mv     | Table             | customers               | Index          | customers_pkey               | 732a1593e99980321e70519dac105e49 | 2025-11-26 18:19:35.773858
          25 |           9 | test_mv     | Materialized View | mv_sales_daily          | Index          | idx_mv_sales_daily_date      | 4d66be207f0c470f85c2109d3340e23c | 2025-11-26 18:19:35.773863
          26 |           9 | test_mv     | Table             | sales_txn               | Reference      | sales_txn_cust_id_fkey       | c11aeff0e53dc6614383720e22b83ac7 | 2025-11-26 18:19:35.778436
          27 |           9 | test_mv     | Table             | sales_txn               | Sequence       | sales_txn_txn_id_seq         | ed48d9f699661cbdf84b30f41cc02dc3 | 2025-11-26 18:19:35.781793
(27 rows)

select metadata_id,snapshot_id,schema_name,object_type,object_type_name,object_subtype,object_subtype_name,object_subtype_details,object_md5,processed_time from pdcd_schema.md5_metadata_staging_functions;

 metadata_id | snapshot_id | schema_name |    object_type    |    object_type_name     | object_subtype | object_subtype_name |                                                                         object_subtype_details                                                                         |            object_md5            |       processed_time
-------------+-------------+-------------+-------------------+-------------------------+----------------+---------------------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------+----------------------------------+----------------------------
           1 |           9 | test_mv     | View              | v_sales_daily_gold_high |                |                     | view_type:VIEW,view_definition: SELECT h.txn_date,                                                                                                                    +| 6d6d23f8efcdadf3c56c238d46ff0703 | 2025-11-26 18:19:36.32096
             |             |             |                   |                         |                |                     |     h.cust_id,                                                                                                                                                        +|                                  |
             |             |             |                   |                         |                |                     |     h.total_amount                                                                                                                                                    +|                                  |
             |             |             |                   |                         |                |                     |    FROM test_mv.v_sales_daily_high h                                                                                                                                  +|                                  |
             |             |             |                   |                         |                |                     |      JOIN test_mv.customers c ON c.cust_id = h.cust_id                                                                                                                +|                                  |
             |             |             |                   |                         |                |                     |   WHERE c.segment::text = 'GOLD'::text;,base_tables:test_mv.customers,view_owner:test_user,dependent_objects:Dependent views: test_mv.v_sales_daily_high               |                                  |
           2 |           9 | test_mv     | View              | v_sales_segment_gold    |                |                     | view_type:VIEW,view_definition: SELECT mv_sales_segment.segment,                                                                                                      +| d99b4025e9d1d2088e995747ff892e9b | 2025-11-26 18:19:36.329664
             |             |             |                   |                         |                |                     |     mv_sales_segment.txn_cnt,                                                                                                                                         +|                                  |
             |             |             |                   |                         |                |                     |     mv_sales_segment.avg_amount,                                                                                                                                      +|                                  |
             |             |             |                   |                         |                |                     |     mv_sales_segment.total_amount                                                                                                                                     +|                                  |
             |             |             |                   |                         |                |                     |    FROM test_mv.mv_sales_segment                                                                                                                                      +|                                  |
             |             |             |                   |                         |                |                     |   WHERE mv_sales_segment.segment::text = 'GOLD'::text;,base_tables:,view_owner:test_user,dependent_objects:Dependent views: test_mv.mv_sales_segment                   |                                  |
           3 |           9 | test_mv     | View              | v_sales_daily_high      |                |                     | view_type:VIEW,view_definition: SELECT mv_sales_daily.txn_date,                                                                                                       +| b7aec55c97b0d53f4d7c131eb6ef2574 | 2025-11-26 18:19:36.32969
             |             |             |                   |                         |                |                     |     mv_sales_daily.cust_id,                                                                                                                                           +|                                  |
             |             |             |                   |                         |                |                     |     mv_sales_daily.txn_cnt,                                                                                                                                           +|                                  |
             |             |             |                   |                         |                |                     |     mv_sales_daily.total_amount                                                                                                                                       +|                                  |
             |             |             |                   |                         |                |                     |    FROM test_mv.mv_sales_daily                                                                                                                                        +|                                  |
             |             |             |                   |                         |                |                     |   WHERE mv_sales_daily.total_amount > 5000::numeric;,base_tables:,view_owner:test_user,dependent_objects:Dependent views: test_mv.mv_sales_daily                       |                                  |
           4 |           9 | test_mv     | Materialized View | mv_sales_segment        |                |                     | view_type:MATERIALIZED VIEW,view_definition: SELECT c.segment,                                                                                                        +| d4142002af707c5ac59d459f6c3966e5 | 2025-11-26 18:19:36.339598
             |             |             |                   |                         |                |                     |     count(*) AS txn_cnt,                                                                                                                                              +|                                  |
             |             |             |                   |                         |                |                     |     avg(s.amount) AS avg_amount,                                                                                                                                      +|                                  |
             |             |             |                   |                         |                |                     |     sum(s.amount) AS total_amount                                                                                                                                     +|                                  |
             |             |             |                   |                         |                |                     |    FROM test_mv.sales_txn s                                                                                                                                           +|                                  |
             |             |             |                   |                         |                |                     |      JOIN test_mv.customers c ON c.cust_id = s.cust_id                                                                                                                +|                                  |
             |             |             |                   |                         |                |                     |   GROUP BY c.segment;,base_tables:test_mv.customers, test_mv.sales_txn,is_ populated:true,view_owner:test_user,dependent_objects:Indexes: idx_mv_sales_segment_segment |                                  |
           5 |           9 | test_mv     | Materialized View | mv_sales_daily          |                |                     | view_type:MATERIALIZED VIEW,view_definition: SELECT s.txn_date,                                                                                                       +| df7187de34addb5f56e86c4410ca98aa | 2025-11-26 18:19:36.339654
             |             |             |                   |                         |                |                     |     s.cust_id,                                                                                                                                                        +|                                  |
             |             |             |                   |                         |                |                     |     count(*) AS txn_cnt,                                                                                                                                              +|                                  |
             |             |             |                   |                         |                |                     |     sum(s.amount) AS total_amount                                                                                                                                     +|                                  |
             |             |             |                   |                         |                |                     |    FROM test_mv.sales_txn s                                                                                                                                           +|                                  |
             |             |             |                   |                         |                |                     |   GROUP BY s.txn_date, s.cust_id;,base_tables:test_mv.sales_txn,is_ populated:true,view_owner:test_user,dependent_objects:Indexes: idx_mv_sales_daily_date             |                                  |
(5 rows)


-- 1. Rename existing MV (versioning)
ALTER MATERIALIZED VIEW test_mv.mv_sales_segment
RENAME TO mv_sales_segment_bkp;

-- 2. Create modified MV (structure change)
CREATE MATERIALIZED VIEW test_mv.mv_sales_segment_v2 AS
SELECT
    c.segment,
    COUNT(*)        AS txn_cnt,
    AVG(s.amount)   AS avg_amount,
    SUM(s.amount)   AS total_amount
FROM test_mv.sales_txn s
JOIN test_mv.customers c ON c.cust_id = s.cust_id
GROUP BY c.segment;

-- 3. Re-point dependent view (structure changed → must DROP & CREATE)
DROP VIEW test_mv.v_sales_segment_gold;

CREATE VIEW test_mv.v_sales_segment_gold AS
SELECT *
FROM test_mv.mv_sales_segment_v2
WHERE segment = 'GOLD';

-- 4. Drop old MV backup
DROP MATERIALIZED VIEW test_mv.mv_sales_segment_bkp;

-- 5. Normalize name back
ALTER MATERIALIZED VIEW test_mv.mv_sales_segment_v2
RENAME TO mv_sales_segment;

-- 6. Rebuild index with new definition
DROP INDEX IF EXISTS test_mv.idx_mv_sales_segment_segment;

CREATE INDEX idx_mv_sales_segment_segment
ON test_mv.mv_sales_segment(segment);

    SELECT * FROM pdcd_schema.load_snapshot_tbl();
    SELECT * FROM pdcd_schema.compare_load_md5_metadata_tbl(ARRAY['test_mv']);
    SELECT * from pdcd_schema.compare_load_md5_metadata_functions(ARRAY['test_mv']);
    TRUNCATE TABLE pdcd_schema.md5_metadata_staging_tbl RESTART IDENTITY CASCADE;
    TRUNCATE TABLE pdcd_schema.md5_metadata_staging_functions RESTART IDENTITY CASCADE;
    SELECT * FROM pdcd_schema.load_md5_metadata_staging_tbl(ARRAY['test_mv']);
    SELECT * from pdcd_schema.load_md5_metadata_staging_functions(ARRAY['test_mv']);

select metadata_id,snapshot_id,schema_name,object_type,object_type_name,object_subtype,object_subtype_name,object_md5,processed_time,change_type FROM pdcd_schema.md5_metadata_tbl;

select metadata_id,snapshot_id,schema_name,object_type,object_type_name,object_subtype,object_subtype_name,object_md5,processed_time from pdcd_schema.md5_metadata_staging_tbl;

select metadata_id,snapshot_id,schema_name,object_type,object_type_name,object_subtype,object_subtype_name,object_subtype_details,object_md5,processed_time from pdcd_schema.md5_metadata_staging_functions;