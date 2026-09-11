# Databricks notebook source
import json
from pyspark.sql.functions import current_timestamp

start_time = spark.sql("SELECT current_timestamp()").collect()[0][0]

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

# Parse JSON safely
table_metadata = json.loads(dbutils.widgets.get("table_metadata").replace("'", '"'))
table_parameters = json.loads(dbutils.widgets.get("table_parameters").replace("'", '"'))

print("Table Metadata:", table_metadata)
print("Table Parameters:", table_parameters)
print(f"Run ID: {run_id}")


# COMMAND ----------

table_id = int(table_metadata['table_id'])
table_name = table_metadata["table_name"]
source_system = table_metadata["source_system"].lower()
source_schema = table_metadata["source_schema"]
source_table = table_metadata["source_table"]
source_path = table_metadata["source_path"]
bronze_schema = table_metadata["bronze_schema"]

load_type = table_parameters["load_type"]

if "watermark_column" in table_parameters.keys():
    watermark_column = table_parameters["watermark_column"]

bronze_table_fqn = f"banking.{bronze_schema}.{table_name}"
print(f"Target Bronze Table: {bronze_table_fqn}")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS banking.bronze;

# COMMAND ----------

# MAGIC %md
# MAGIC **Metadata Entry**

# COMMAND ----------

entry_exists = spark.sql(f"""
    SELECT 1
    FROM banking.metadata.pipeline_runs
    WHERE run_id = {run_id} AND table_id = {table_id}
""").count() > 0

if entry_exists:
    spark.sql(f"""
        UPDATE banking.metadata.pipeline_runs
        SET
            layer = 'Silver',
            start_time = TIMESTAMP('{start_time}'),
            end_time = NULL,
            status = 'INPROGRESS',
            number_of_records = NULL,
            error_message = NULL
        WHERE run_id = {run_id} AND table_id = {table_id}
    """)
else:
    spark.sql(f"""
        INSERT INTO banking.metadata.pipeline_runs
        VALUES (
            {run_id},
            {table_id},
            'Silver',
            TIMESTAMP('{start_time}'),
            NULL,  -- end time
            'INPROGRESS',
            NULL, --number of records
            NULL -- error message
        )
    """)


# COMMAND ----------

#getting last watermark for filtering ; 
last_watermark = None
 
#there are no watermarks for the full load tables ; only for APPEND or MERGE load_types because these are incremental loads;
if load_type in ["APPEND", "MERGE"] and watermark_column:
    watermark_df = spark.sql(f"""
        SELECT last_watermark_value
        FROM banking.metadata.table_watermarks
        WHERE table_id = {table_id}
    """)
    
    if watermark_df.count() > 0:
        last_watermark = watermark_df.first()["last_watermark_value"]

print("Last Watermark:", last_watermark)



# COMMAND ----------



# DBTITLE 1,Cell 7
try:
    if source_system == "sqlserver":
        #standard production way of passing in the configurations from the secret scope ; 
        secret_json = dbutils.secrets.get(
            scope="neobank-scope",
            key="sqlserver-connection-json"
        )

        config = json.loads(secret_json)

        jdbc_url = f"jdbc:sqlserver://tulover-server.database.windows.net:1433;database={config["database"]};user={config["user"]};password={config["password"]};encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=30;authentication=ActiveDirectoryPassword"
        #main jdbc url to load the data from the external source ; this is what directs the driver to get the data from the source 

        jdbc_properties = {
            "user": config["user"],
            "password": config["password"],
            "driver": config["driver"]
        }
        
        # just building the queries
        if load_type in ["APPEND", "MERGE"] and last_watermark:     #incremental load
            query = f"""
            (SELECT * FROM {source_schema}.{source_table}
             WHERE {watermark_column} > '{last_watermark}') AS src
            """
        else:   #full load
            query = f"(SELECT * FROM {source_schema}.{source_table}) AS src"
        

        source_df = spark.read.jdbc(
            url=jdbc_url,
            table=query,
            properties=jdbc_properties
        )

    elif source_system == "blob":

        source_df = (
            spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .option(
                "cloudFiles.schemaLocation",
                f"/Volumes/banking/source/volume/_schema/{table_name}"
            )       # file schema inferred by autoloader ; 
            .option("header", "true")
            .load(source_path)
        )
    #cloudFiles => use of AUTOLOADER ; means it loads and processes new data files incrementally/efficiently as they arrive in cloud storage

    else:
        raise ValueError("Unsupported source_system")


    source_df = source_df.withColumn("insert_timestamp", current_timestamp())
    #inserting a new column of the timestamp ; 

    # reading part is done ; writing part starts ; 

    if source_system == "blob":

        (
            source_df.writeStream
            .format("delta")
            .option(
                "checkpointLocation",
                f"/Volumes/banking/source/volume/_checkpoints/{table_name}"
            )            # the checkpoint location is really important cause that checkpoint is where the autoloader stores which files have already been processed and which haven't been processed yet ; this is how it knows which files to process incrementally/effieciently ; 
            .outputMode("append")
            .trigger(availableNow=True)
            .toTable(bronze_table_fqn)
        )

        records_read = None  # streaming, can't count records here

    else:       #sql server data ;

        (
            source_df.write
            .format("delta")
            .mode("append")     #we are continuously appending the raw data in bronze layer ; 
            .saveAsTable(bronze_table_fqn)
        )
        #so bronze layer is continuously dump of the data only ; 
        records_read = source_df.count()

    print("Source → Bronze Load Completed Successfully.")
    print("Watermark will be updated after Silver load.")

except Exception as e:
    end_time = spark.sql("SELECT current_timestamp()").collect()[0][0]
    error_message = str(e)

    spark.sql(f"""
        UPDATE banking.metadata.pipeline_runs
        SET
            end_time = TIMESTAMP('{end_time}'),
            status = 'FAILED',
            error_message = {'NULL' if not error_message else "'" + error_message.replace("'", "") + "'"}
        WHERE table_id = {table_id} AND run_id = {run_id} 
    """)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from banking.metadata.pipeline_runs;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from banking.bronze.customers;