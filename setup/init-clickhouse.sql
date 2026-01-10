CREATE DATABASE IF NOT EXISTS data_pipeline;
CREATE DATABASE IF NOT EXISTS data_pipeline_staging;
CREATE DATABASE IF NOT EXISTS data_pipeline_staging_snapshots;
CREATE DATABASE IF NOT EXISTS data_pipeline_marts;

CREATE USER pipeline_user IDENTIFIED WITH sha256_password BY 'aer2DCCpt1nf';

GRANT CREATE TABLE, CREATE DATABASE, INSERT, UPDATE, ALTER, DROP, SELECT, TRUNCATE
        ON *.* TO pipeline_user;

CREATE TABLE IF NOT EXISTS data_pipeline.raw_savings_plans (
    plan_id String,
    product_type String,
    customer_uid String,
    amount Float64,
    frequency String,
    start_date Date,
    end_date Nullable(Date),
    status String,
    created_at DateTime,
    updated_at DateTime,
    deleted_at Nullable(DateTime)
) ENGINE = MergeTree()
ORDER BY (plan_id, updated_at);

CREATE TABLE IF NOT EXISTS data_pipeline.raw_savings_transactions (
    txn_id String,
    plan_id String,
    amount Float64,
    currency String,
    side String,
    rate Float64,
    txn_timestamp DateTime,
    updated_at DateTime,
    deleted_at Nullable(DateTime)
) ENGINE = MergeTree()
PARTITION BY toStartOfMonth(txn_timestamp) 
ORDER BY (txn_id, updated_at, txn_timestamp);

CREATE TABLE IF NOT EXISTS data_pipeline.raw_users (
    _id String,
    user_id String,
    firstName String,
    lastName String,
    occupation String,
    state String
) ENGINE = MergeTree()
ORDER BY (user_id);