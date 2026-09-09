-- Databricks notebook source
CREATE CATALOG IF NOT EXISTS banking;

CREATE SCHEMA IF NOT EXISTS banking.metadata;

CREATE TABLE IF NOT EXISTS banking.metadata.tables (
    table_id            INT,
    table_name          STRING,
    source_system       STRING,        -- sqlserver / blob
    source_schema       STRING,        -- dbo for the sql server (null for blob)
    source_table        STRING,        -- table name (null for blob)
    source_path         STRING,        -- blob path if object storage(null for sqlserver)
    target_layer        STRING,        -- silver/gold
    bronze_schema       STRING,        -- bronze
    silver_schema       STRING,        -- silver
    gold_schema         STRING,        -- gold
    active_flag         BOOLEAN,
    load_order          INT,
    created_at          TIMESTAMP
)
USING DELTA;
-- this table would store all of the metadata of the tables 


--this table would store certain parameters for each table ; here foreign key table_id
CREATE TABLE IF NOT EXISTS banking.metadata.table_parameters (
    table_id            INT,
    parameter_name      STRING,        -- load_type / primary_key / watermark_column
    parameter_value     STRING,
    created_at          TIMESTAMP
)
USING DELTA;
-- instead of writing each parameter of the table along in the 'tables' table, we go for normalization ; 
-- normalization keeps the data clean and easy to maintain ; 


-- metadata.table_watermarks
-- Stores last successful watermark per table ; watermark for incremental loading

CREATE TABLE IF NOT EXISTS banking.metadata.table_watermarks (
    table_id                INT,
    last_watermark_value    STRING,     -- flexible type storage
    last_updated_at         TIMESTAMP,
    last_run_id             BIGINT
)
USING DELTA         --writing delta explicity even though delta tables are default for zero ambiguity
PARTITIONED BY (table_id);
-- here delta table will physically parition data in separate folders based on table_id ; this concept is called 'PARTITION PRUNING' and this optimizes the query because when data is queried it will only look into the folders that match the partition value ; how do we know that we are partitioning the value here ? --> cause table watermark is always fetched as per the table_id ; this is the reason we partition the data beforehand using table_id ; 


-- this is  audit/logging table cause pipelines fail and debugging is needed
-- one of the good data engineering practices is maintaining the audit table for all the pipelines ; 

CREATE TABLE IF NOT EXISTS banking.metadata.pipeline_runs (
    run_id              BIGINT,
    table_id            INT,
    layer               STRING,        -- Bronze / Silver / Gold
    start_time          TIMESTAMP,
    end_time            TIMESTAMP,
    status              STRING,        -- success or failure ?
    number_of_records     BIGINT,
    error_message       STRING
)
USING DELTA
PARTITIONED BY (table_id);

-- COMMAND ----------


create schema if not exists banking.source;
-- creating volume of blob storage data landing ; 
create volume if not exists banking.source.volume;

-- COMMAND ----------

INSERT INTO banking.metadata.tables VALUES
(1, 'customers', 'sqlserver', 'banking', 'customers', NULL, 'silver', 'bronze', 'silver', NULL, TRUE, 1, current_timestamp()),

(2, 'accounts', 'sqlserver', 'banking', 'accounts', NULL, 'silver', 'bronze', 'silver', NULL, TRUE, 2, current_timestamp()),

(3, 'transactions', 'sqlserver', 'banking', 'transactions', NULL, 'silver', 'bronze', 'silver', NULL, TRUE, 3, current_timestamp()),

-- Branches (SQL Server - Full Load)
(4, 'branches', 'sqlserver', 'banking', 'branches', NULL, 'silver', 'bronze', 'silver', NULL, TRUE, 4, current_timestamp()),

--5 & 6 being blob csvs
(5, 'credit_bureau_reports', 'blob', NULL, NULL,
 '/Volumes/banking/source/volume/credit_bureau_reports/', 'silver',
 'bronze', 'silver', NULL, TRUE, 5, current_timestamp()),

(6, 'payment_gateway_logs', 'blob', NULL, NULL,
 '/Volumes/banking/source/volume/payment_gateway_logs/', 'silver',
 'bronze', 'silver', NULL, TRUE, 6, current_timestamp());

-- COMMAND ----------

INSERT INTO banking.metadata.table_parameters VALUES

-- customers table
(1, 'load_type', 'MERGE', current_timestamp()), -- load_type MERGE means UPSERT operation ; 
(1, 'primary_key', 'customer_id', current_timestamp()),
(1, 'watermark_column', 'updated_at', current_timestamp()),

-- accounts table
(2, 'load_type', 'MERGE', current_timestamp()),
(2, 'primary_key', 'account_id', current_timestamp()),
(2, 'watermark_column', 'updated_at', current_timestamp()),

-- transactions table
(3, 'load_type', 'APPEND', current_timestamp()),
(3, 'primary_key', 'txn_id', current_timestamp()),
(3, 'watermark_column', 'txn_timestamp', current_timestamp()),

-- branches table
(4, 'load_type', 'FULL', current_timestamp()),
(4, 'primary_key', 'branch_code', current_timestamp()),

-- credit bureau reports table
(5, 'load_type', 'MERGE', current_timestamp()),
(5, 'primary_key', 'customer_id', current_timestamp()),
(5, 'watermark_column', 'bureau_pull_date', current_timestamp()),

-- payment gateway logs table
(6, 'load_type', 'APPEND', current_timestamp()),
(6, 'primary_key', 'txn_id', current_timestamp()),
(6, 'watermark_column', 'processed_timestamp', current_timestamp());

-- COMMAND ----------

-- watermarks initiated ; 

INSERT INTO banking.metadata.table_watermarks VALUES
(1, '1900-01-01 00:00:00', current_timestamp(), NULL),
(2, '1900-01-01 00:00:00', current_timestamp(), NULL),
(3, '1900-01-01 00:00:00', current_timestamp(), NULL),
(5, '1900-01-01 00:00:00', current_timestamp(), NULL),
(6, '1900-01-01 00:00:00', current_timestamp(), NULL);

-- COMMAND ----------

INSERT INTO banking.metadata.tables
VALUES
(8,'branch_performance','silver',NULL,NULL,NULL,'gold',NULL,NULL,'gold',TRUE,2,current_timestamp()),

(9,'transaction_channel_summary','silver',NULL,NULL,NULL,'gold',NULL,NULL,'gold',TRUE,3,current_timestamp()),

(10,'daily_bank_kpi','silver',NULL,NULL,NULL,'gold',NULL,NULL,'gold',TRUE,4,current_timestamp()),

(11,'risk_customer_summary','silver',NULL,NULL,NULL,'gold',NULL,NULL,'gold',TRUE,1,current_timestamp());