# Databricks notebook source
import json
import time
from pyspark.sql.functions import current_timestamp, col, max as spark_max
from delta.tables import DeltaTable

status = "SUCCESS"
error_message = None
records_read = 0
records_written = 0

dbutils.widgets.text(
    "table_metadata",
    "{'table_id': '1', 'table_name': 'customers', 'source_system': 'sqlserver', "
    "'source_schema': 'banking', 'source_table': 'customers', "
    "'source_path': '', 'bronze_schema': 'bronze', "
    "'silver_schema': 'silver', 'active_flag': 'True', "
    "'load_order': '1', 'created_at': '2026-02-18 13:23:37.053711'}"
)

dbutils.widgets.text(
    "table_parameters",
    "{'load_type': 'MERGE', "
    "'primary_key': 'customer_id', "
    "'watermark_column': 'updated_at'}"
)

dbutils.widgets.text(
    "run_id",
    "472519629468310"
)
run_id=dbutils.widgets.get("run_id")

table_metadata = json.loads(dbutils.widgets.get("table_metadata").replace("'", '"'))
table_parameters = json.loads(dbutils.widgets.get("table_parameters").replace("'", '"'))


table_id = int(table_metadata["table_id"])
table_name = table_metadata["table_name"]
bronze_schema = table_metadata["bronze_schema"]
silver_schema = table_metadata["silver_schema"]

load_type = table_parameters.get("load_type")
primary_key = table_parameters.get("primary_key")
watermark_column = table_parameters.get("watermark_column")

bronze_table = f"banking.{bronze_schema}.{table_name}"
silver_table = f"banking.{silver_schema}.{table_name}"

print(f"Processing Table: {table_name}")
print(f"Load Type: {load_type}")
print(f"Run ID: {run_id}")



# COMMAND ----------

try:

    bronze_df = spark.table(bronze_table)

    if load_type in ["APPEND", "MERGE"] and watermark_column:

        watermark_df = spark.sql(f"""
            SELECT last_watermark_value
            FROM banking.metadata.table_watermarks
            WHERE table_id = {table_id}
        """)

        last_watermark = None
        if watermark_df.count() > 0:
            last_watermark = watermark_df.first()["last_watermark_value"]

        if last_watermark:
            bronze_df = bronze_df.filter(
                col(watermark_column) > last_watermark
            )

    records_read = bronze_df.count()
    print("Records to process:", records_read)


    bronze_df = (
        bronze_df
        .withColumn("insert_timestamp", current_timestamp())
        .withColumn("update_timestamp", current_timestamp())
    )

    spark.sql("create schema if not exists banking.silver")

    if not spark.catalog.tableExists(silver_table):
        (
            bronze_df
            .write
            .format("delta")
            .mode("overwrite")
            .saveAsTable(silver_table)
        )
        records_written = records_read

    else:

        if load_type == "FULL":

            (
                bronze_df
                .write
                .format("delta")
                .mode("overwrite")
                .option("overwriteSchema", "true")
                .saveAsTable(silver_table)
            )
            records_written = records_read

        elif load_type == "APPEND":

            (
                bronze_df
                .write
                .format("delta")
                .mode("append")
                .saveAsTable(silver_table)
            )
            records_written = records_read

        elif load_type == "MERGE":

            if not primary_key:
                raise ValueError("Primary key required for MERGE.")
            # PK is always required for UPSERT/MERGE operation
            # remember how bronze layer had a continuous append of the data as it is dump of raw data ; the record of the historical data
            # has to be maintained for the data lineage in it; for SILVER layer though, UPSERT operation is used ;

            delta_table = DeltaTable.forName(spark, silver_table)

            merge_condition = f"t.{primary_key} = s.{primary_key}"

            (
                delta_table.alias("t")
                .merge(
                    bronze_df.alias("s"),
                    merge_condition
                )
                .whenMatchedUpdateAll()     # update if matches
                .whenNotMatchedInsertAll()  #insert if not matches
                .execute()
            )

            records_written = records_read

        else:
            raise ValueError("Unsupported load_type")


#updating watermark values

    if load_type in ["APPEND", "MERGE"] and watermark_column:

        max_value = bronze_df.agg(
            spark_max(col(watermark_column))
        ).collect()[0][0]

        if max_value:

            spark.sql(f"""
                MERGE INTO banking.metadata.table_watermarks t
                USING (SELECT {table_id} AS table_id) s
                ON t.table_id = s.table_id
                WHEN MATCHED THEN UPDATE SET        -- updating if the watermark value is there ; 
                    last_watermark_value = '{max_value}',
                    last_updated_at = current_timestamp()
                WHEN NOT MATCHED THEN           -- inserting if the watermark value is not there ; 
                    INSERT (table_id, last_watermark_value, last_updated_at)
                    VALUES ({table_id}, '{max_value}', current_timestamp())
            """)

    print("Silver Load Completed Successfully.")


except Exception as e:

    status = "FAILED"
    error_message = str(e)
    print("Error Occurred:", error_message)
    raise


finally:

    end_time = spark.sql("SELECT current_timestamp()").collect()[0][0]

    spark.sql(f"""
        UPDATE banking.metadata.pipeline_runs
        SET
            end_time = TIMESTAMP('{end_time}'),
            status = '{status}',
            number_of_records = {records_read},
            error_message = {'NULL' if not error_message else "'" + error_message.replace("'", "") + "'"}
        WHERE table_id = {table_id} and run_id = {run_id}
    """)

    print("Audit record inserted.")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from banking.metadata.table_watermarks;

# COMMAND ----------

