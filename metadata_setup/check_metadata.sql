-- Databricks notebook source

select * from banking.metadata.tables

-- COMMAND ----------

select * from banking.metadata.table_parameters

-- COMMAND ----------

select t.table_name, table_parameters.parameter_value from banking.metadata.tables t 
join banking.metadata.table_parameters where t.table_id = table_parameters.table_id and table_parameters.parameter_name = 'load_type'


-- have to internalize how the sql is working here ; why is only this "select t.table_name, table_parameters.parameter_value" not working ?

-- COMMAND ----------

select * from banking.metadata.table_watermarks

-- COMMAND ----------

SELECT * FROM banking.metadata.pipeline_runs

-- COMMAND ----------

