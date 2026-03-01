-- Set session-level optimizations
\set ON_ERROR_STOP on
SET compupdate OFF;
SET query_group TO 'etl_batch_high_priority';

-- Start Transaction
BEGIN;

-- 1. Create a dynamic Temp Table based on the target schema
-- This ensures the staging structure always matches the target
DROP TABLE IF EXISTS stage_temp;
CREATE TEMP TABLE stage_temp (LIKE ${TGT_TABLE});

\echo 'Loading data from S3 for period starting: ${LAST_SUCCESSFUL_LOAD}'

-- 2. COPY command using a Manifest and Instance Profile
-- We use variables for credentials and paths for flexibility
COPY stage_temp
FROM '${SRC_BUCKET}manifest_${LOAD_DATE}.json'
IAM_ROLE '${IAM_ROLE}'
MANIFEST
FORMAT AS PARQUET
STATUPDATE ON;

-- 3. Complex Upsert Logic (Handling Multiple PKs)
-- We delete records from Target that exist in Stage to handle updates
DELETE FROM ${TGT_TABLE}
USING stage_temp
WHERE ${TGT_TABLE}.${PK_COLUMNS_1} = stage_temp.${PK_COLUMNS_1}
  AND ${TGT_TABLE}.${PK_COLUMNS_2} = stage_temp.${PK_COLUMNS_2};

-- 4. Insert the new/updated batch
INSERT INTO ${TGT_TABLE}
SELECT * FROM stage_temp;

-- 5. Audit Logging (Essential for production)
-- Captures row counts and timestamps for data lineage
INSERT INTO dw_metadata.etl_load_log (
    table_name, 
    load_start_time, 
    rows_inserted, 
    status
) 
VALUES (
    '${TGT_TABLE}', 
    '${LOAD_TIMESTAMP}', 
    (SELECT COUNT(*) FROM stage_temp), 
    'SUCCESS'
);

COMMIT;

-- Clean up
DROP TABLE stage_temp;
\echo 'ETL Pipeline finished for ${TGT_TABLE}';