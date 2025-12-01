CREATE OR REPLACE FUNCTION pdcd_schema.load_md5_metrics_tbl()
RETURNS VOID
LANGUAGE plpgsql
AS $$
DECLARE
    v_snapshot_id INT;
BEGIN
    -- Use latest snapshot for tagging
    SELECT MAX(snapshot_id)
    INTO v_snapshot_id
    FROM pdcd_schema.md5_metadata_tbl;

    INSERT INTO pdcd_schema.md5_metrics_tbl
        (snapshot_id, metric_name, metric_value, metrics_time)

    SELECT
        v_snapshot_id,
        m.metric_name,
        m.metric_value,
        clock_timestamp()
    FROM (

        SELECT 'Schemas Monitored' AS metric_name,
               COUNT(DISTINCT schema_name) AS metric_value
        FROM pdcd_schema.md5_metadata_tbl

        UNION ALL
        SELECT 'Schemas With Changes',
               COUNT(DISTINCT schema_name)
        FROM pdcd_schema.md5_metadata_tbl
        WHERE change_type <> 'UNCHANGED'

        UNION ALL
        SELECT 'Tables With Changes',
               COUNT(DISTINCT object_type_name)
        FROM pdcd_schema.md5_metadata_tbl
        WHERE object_type = 'TABLE'
          AND change_type <> 'UNCHANGED'

        UNION ALL
        SELECT 'Total Change Operations',
               COUNT(*)
        FROM pdcd_schema.md5_metadata_tbl
        WHERE change_type <> 'UNCHANGED'


        ---------------- TABLES ----------------
        UNION ALL SELECT 'Tables Added',
        COUNT(*) FROM pdcd_schema.md5_metadata_tbl
        WHERE object_type='TABLE' AND change_type='ADDED'

        UNION ALL SELECT 'Tables Dropped',
        COUNT(*) FROM pdcd_schema.md5_metadata_tbl
        WHERE object_type='TABLE' AND change_type='DELETED'

        UNION ALL SELECT 'Tables Modified',
        COUNT(*) FROM pdcd_schema.md5_metadata_tbl
        WHERE object_type='TABLE' AND change_type='MODIFIED'

        UNION ALL SELECT 'Tables Renamed',
        COUNT(*) FROM pdcd_schema.md5_metadata_tbl
        WHERE object_type='TABLE' AND change_type='RENAMED'


        ---------------- COLUMNS ----------------
        UNION ALL SELECT 'Columns Added',
        COUNT(*) FROM pdcd_schema.md5_metadata_tbl
        WHERE object_subtype='Column' AND change_type='ADDED'

        UNION ALL SELECT 'Columns Dropped',
        COUNT(*) FROM pdcd_schema.md5_metadata_tbl
        WHERE object_subtype='Column' AND change_type='DELETED'

        UNION ALL SELECT 'Columns Modified',
        COUNT(*) FROM pdcd_schema.md5_metadata_tbl
        WHERE object_subtype='Column' AND change_type='MODIFIED'

        UNION ALL SELECT 'Columns Renamed',
        COUNT(*) FROM pdcd_schema.md5_metadata_tbl
        WHERE object_subtype='Column' AND change_type='RENAMED'


        ---------------- CONSTRAINTS ----------------
        UNION ALL SELECT 'Constraints Added',
        COUNT(*) FROM pdcd_schema.md5_metadata_tbl
        WHERE object_subtype='Constraint' AND change_type='ADDED'

        UNION ALL SELECT 'Constraints Dropped',
        COUNT(*) FROM pdcd_schema.md5_metadata_tbl
        WHERE object_subtype='Constraint' AND change_type='DELETED'

        UNION ALL SELECT 'Constraints Modified',
        COUNT(*) FROM pdcd_schema.md5_metadata_tbl
        WHERE object_subtype='Constraint' AND change_type='MODIFIED'

        UNION ALL SELECT 'Constraints Renamed',
        COUNT(*) FROM pdcd_schema.md5_metadata_tbl
        WHERE object_subtype='Constraint' AND change_type='RENAMED'


        ---------------- TRIGGERS ----------------
        UNION ALL SELECT 'Triggers Added',
        COUNT(*) FROM pdcd_schema.md5_metadata_tbl
        WHERE object_subtype='Trigger' AND change_type='ADDED'

        UNION ALL SELECT 'Triggers Dropped',
        COUNT(*) FROM pdcd_schema.md5_metadata_tbl
        WHERE object_subtype='Trigger' AND change_type='DELETED'

        UNION ALL SELECT 'Triggers Modified',
        COUNT(*) FROM pdcd_schema.md5_metadata_tbl
        WHERE object_subtype='Trigger' AND change_type='MODIFIED'

        UNION ALL SELECT 'Triggers Renamed',
        COUNT(*) FROM pdcd_schema.md5_metadata_tbl
        WHERE object_subtype='Trigger' AND change_type='RENAMED'


        ---------------- SEQUENCES ----------------
        UNION ALL SELECT 'Sequences Added',
        COUNT(*) FROM pdcd_schema.md5_metadata_tbl
        WHERE object_type='SEQUENCE' AND change_type='ADDED'

        UNION ALL SELECT 'Sequences Dropped',
        COUNT(*) FROM pdcd_schema.md5_metadata_tbl
        WHERE object_type='SEQUENCE' AND change_type='DELETED'

        UNION ALL SELECT 'Sequences Modified',
        COUNT(*) FROM pdcd_schema.md5_metadata_tbl
        WHERE object_type='SEQUENCE' AND change_type='MODIFIED'

        UNION ALL SELECT 'Sequences Renamed',
        COUNT(*) FROM pdcd_schema.md5_metadata_tbl
        WHERE object_type='SEQUENCE' AND change_type='RENAMED'


        ---------------- REFERENCES ----------------
        UNION ALL SELECT 'References Added',
        COUNT(*) FROM pdcd_schema.md5_metadata_tbl
        WHERE object_subtype='Reference' AND change_type='ADDED'

        UNION ALL SELECT 'References Dropped',
        COUNT(*) FROM pdcd_schema.md5_metadata_tbl
        WHERE object_subtype='Reference' AND change_type='DELETED'

        UNION ALL SELECT 'References Modified',
        COUNT(*) FROM pdcd_schema.md5_metadata_tbl
        WHERE object_subtype='Reference' AND change_type='MODIFIED'

        UNION ALL SELECT 'References Renamed',
        COUNT(*) FROM pdcd_schema.md5_metadata_tbl
        WHERE object_subtype='Reference' AND change_type='RENAMED'


        ---------------- INDEXES ----------------
        UNION ALL SELECT 'Indexes Added',
        COUNT(*) FROM pdcd_schema.md5_metadata_tbl
        WHERE object_subtype='Index' AND change_type='ADDED'

        UNION ALL SELECT 'Indexes Dropped',
        COUNT(*) FROM pdcd_schema.md5_metadata_tbl
        WHERE object_subtype='Index' AND change_type='DELETED'

        UNION ALL SELECT 'Indexes Modified',
        COUNT(*) FROM pdcd_schema.md5_metadata_tbl
        WHERE object_subtype='Index' AND change_type='MODIFIED'

        UNION ALL SELECT 'Indexes Renamed',
        COUNT(*) FROM pdcd_schema.md5_metadata_tbl
        WHERE object_subtype='Index' AND change_type='RENAMED'

    ) AS m;

END;
$$;

---======== Table: pdcd_schema.md5_metrics_tbl ========
-- CREATE TABLE pdcd_schema.md5_metrics_tbl (
--     metrics_id     BIGSERIAL PRIMARY KEY,
--     snapshot_id    INT NOT NULL,
--     metric_name    TEXT NOT NULL,
--     metric_value   INT NOT NULL,
--     metrics_time   TIMESTAMP DEFAULT clock_timestamp()
-- );


-- \i "/Users/manoj_anumalla/Documents/GitHub/Sql_Functions/pcdc_tracking/sql_functions/load_and_compare/load_md5_metrics_tbl.sql"

-- SELECT pdcd_schema.load_md5_metrics_tbl();
