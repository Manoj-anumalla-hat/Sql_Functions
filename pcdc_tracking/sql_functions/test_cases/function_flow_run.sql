TRUNCATE TABLE pdcd_schema.md5_metadata_tbl RESTART IDENTITY CASCADE;
TRUNCATE TABLE pdcd_schema.md5_metadata_staging_tbl RESTART IDENTITY CASCADE;
TRUNCATE TABLE pdcd_schema.md5_metadata_staging_functions RESTART IDENTITY CASCADE;
TRUNCATE TABLE pdcd_schema.snapshot_tbl RESTART IDENTITY CASCADE;

-- TRUNCATE TABLE pdcd_schema.md5_metrics_tbl RESTART IDENTITY CASCADE;

SELECT * FROM pdcd_schema.load_snapshot_tbl();
SELECT * FROM pdcd_schema.load_md5_metadata_tbl(ARRAY['sales','hr','inventory']);
SELECT * FROM pdcd_schema.load_md5_metadata_staging_tbl(ARRAY['sales','hr','inventory']);
SELECT * from pdcd_schema.load_md5_metadata_staging_functions(ARRAY['sales','hr','inventory']);
-- SELECT * from pdcd_schema.load_md5_metrics_tbl();


SELECT * FROM pdcd_schema.load_snapshot_tbl();
SELECT * FROM pdcd_schema.compare_load_md5_metadata_tbl(ARRAY['sales','hr','inventory']);
SELECT * from pdcd_schema.compare_load_md5_metadata_functions(ARRAY['sales','hr','inventory']);
TRUNCATE TABLE pdcd_schema.md5_metadata_staging_tbl RESTART IDENTITY CASCADE;
TRUNCATE TABLE pdcd_schema.md5_metadata_staging_functions RESTART IDENTITY CASCADE;
SELECT * FROM pdcd_schema.load_md5_metadata_staging_tbl(ARRAY['sales','hr','inventory']);
SELECT * from pdcd_schema.load_md5_metadata_staging_functions(ARRAY['sales','hr','inventory']);
-- SELECT * from pdcd_schema.load_md5_metrics_tbl();


select metadata_id,snapshot_id,schema_name,object_type,object_type_name,object_subtype,object_subtype_name,object_md5,processed_time,change_type FROM pdcd_schema.md5_metadata_tbl;

metadata_id | snapshot_id | schema_name | object_type |  object_type_name | object_subtype | object_subtype_name | object_subtype_details  |  object_md5  | processed_time | change_type

Schemas Monitored
Schemas with Changes
Tables With Changes
Total Change Operations(Total tables objects affected)
Tables Added
Tables Dropped
Tables Modified
Tables Renamed
Columns Added
Columns Dropped
Columns Modified
Columns Renamed
Constraints Added
Constraints Dropped
Constraints Modified
Constraints Renamed
......follow same for Triggers,sequences,References,Indexes



SELECT pdcd_schema.load_md5_metrics_tbl();
ERROR:  null value in column "snapshot_id" of relation "CREATE TABLE pdcd_schema.md5_metrics_tbl (
    metrics_id      BIGSERIAL PRIMARY KEY,
    snapshot_id     INT NOT NULL,
    metric_name     TEXT NOT NULL,
    metric_value    INT NOT NULL,
    generated_time  TIMESTAMP DEFAULT clock_timestamp()
);" violates not-null constraint
DETAIL:  Failing row contains (4, null, Schemas Monitored, 3, 2025-11-20 21:10:11.956384).
CONTEXT:  SQL statement "INSERT INTO pdcd_schema.md5_metrics_tbl (metric_name, metric_value)


consider all the snapshots data from md5_metadata_tbl table.

CREATE TABLE pdcd_schema.md5_metrics_tbl (
    metrics_id      BIGSERIAL PRIMARY KEY,
    snapshot_id     INT NOT NULL,
    metric_name     TEXT NOT NULL,
    metric_value    INT NOT NULL,
    generated_time  TIMESTAMP DEFAULT clock_timestamp()
);
