CREATE CATALOG IF NOT EXISTS prod_fin_catalog;
USE CATALOG prod_fin_catalog;

CREATE SCHEMA IF NOT EXISTS landing;
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;
CREATE SCHEMA IF NOT EXISTS audit;

CREATE VOLUME IF NOT EXISTS prod_fin_catalog.landing.raw_api_json
COMMENT 'Managed volume for raw REST API JSON payloads and streaming checkpoints';

CREATE TABLE IF NOT EXISTS prod_fin_catalog.audit.pipeline_audit_log (
    batch_id STRING,
    layer STRING,
    source_count BIGINT,
    inserted_count BIGINT,
    updated_count BIGINT,
    status STRING,
    execution_ts TIMESTAMP
) USING DELTA;

CREATE TABLE IF NOT EXISTS prod_fin_catalog.silver.dim_customer_scd2 (
    customer_sk STRING COMMENT 'Deterministic Surrogate Key: SHA2(customer_id || effective_start_ts)',
    customer_id STRING COMMENT 'Natural Business Key from API',
    full_name STRING,
    email STRING,
    credit_tier STRING,
    credit_limit DECIMAL(18,2),
    risk_score INT,
    address_city STRING,
    row_hash STRING COMMENT 'SHA-256 hash of tracked mutable attributes for fast change detection',
    effective_start_ts TIMESTAMP,
    effective_end_ts TIMESTAMP,
    is_current BOOLEAN,
    batch_id STRING
)
USING DELTA
CLUSTER BY (customer_id, is_current)
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);

CREATE OR REPLACE FUNCTION prod_fin_catalog.silver.mask_email(email STRING)
RETURN CASE 
    WHEN is_account_group_member('compliance_auditors') THEN email
    ELSE regexp_replace(email, '(^[^@]{2})[^@]+(@.*$)', '$1****$2')
END;

ALTER TABLE prod_fin_catalog.silver.dim_customer_scd2 
ALTER COLUMN email SET MASK prod_fin_catalog.silver.mask_email;
