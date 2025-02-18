# Databricks notebook source
# Add python code to path to import python helper functions
import sys, os
sys.path.append(os.path.abspath('/src/PythonHelperFunctions'))

# COMMAND ----------

# import libraries
import dlt
# import python helper functions
from pyspark.sql.functions import current_timestamp, expr
from helper_functions import apply_schema
from schema_definitions import silver_schema

# COMMAND ----------

# extract required pipeline config parameters
storageAccountUrl = spark.conf.get("storageAccountUrl")

# COMMAND ----------

@dlt.view(
  name=f"vw_bronze_layer",
  comment="View to read bronze data"
)
@dlt.expect_or_fail("valid_wind_direction", "wind_direction BETWEEN 0 AND 359")
def bronze_data():
  # read raw data and adds metadata columns
  bronze_df = spark.readStream.format("cloudFiles")\
              .option("cloudFiles.format", "csv")\
              .load(f"abfss://data@{storageAccountUrl}/Colibri/data_group_*.csv", header=True, inferSchema=True, sep=",")\
              .withColumn("load_timestamp", current_timestamp())\
              .withColumn("modified_timestamp", expr("_metadata.file_modification_time"))

  return bronze_df

# COMMAND ----------

@dlt.table(
  name=f"silver_layer",
  comment=f"Table to store silver layer"
)
def silver_data():
  # read from temp table to continue the process only if all DQs are satisfied
  bronze_df = dlt.readStream("vw_bronze_layer")

  # Apply schema
  silver_df = apply_schema(bronze_df, silver_schema, False)

  return silver_df
  
