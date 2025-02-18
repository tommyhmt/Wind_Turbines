# Databricks notebook source
# Add python code to path to import python helper functions
import sys, os
sys.path.append(os.path.abspath('/src/PythonHelperFunctions'))

# COMMAND ----------

# import libraries
import dlt
# import python helper functions
from pyspark.sql.functions import to_date, min, max, sum, avg, stddev, col, lit
from helper_functions import apply_schema
from schema_definitions import gold_schema

# COMMAND ----------

# extract required pipeline config parameters
catalog = spark.conf.get("catalog")

# COMMAND ----------

@dlt.table(
  name=f"gold_layer",
  comment=f"Table to store gold layer"
)
@dlt.expect("anomaly", "anomaly = 'false'")
def gold_data():
  # read from silver layer
  silver_df = spark.table(f"{catalog}.cleansed.silver_layer")

  # Calculates min, max, sum for power_output
  gold_df = silver_df.withColumn("date", to_date("timestamp", "yyyy-MM-dd"))\
                    .groupBy("date", "turbine_id")\
                    .agg(min("power_output").alias("min_power_output"),\
                        max("power_output").alias("max_power_output"),\
                        sum("power_output").alias("avg_power_output"),\
                        sum("power_output").alias("sum_power_output"))

  # Calculates avg, stddev for power_output
  anomaly_df = gold_df.groupBy("date")\
                      .agg(avg("sum_power_output").alias("avg_of_sum_of_power_output"),\
                          stddev("sum_power_output").alias("stddev_power_output"))

  # Joins anomaly_df to gold_df
  gold_df = gold_df.alias("g").join(anomaly_df.alias("a"), ["date"]).select("g.*", "a.avg_of_sum_of_power_output", "a.stddev_power_output")

  # Identifies anomaly
  gold_df = gold_df.withColumn("anomaly", (col("sum_power_output") < col("avg_of_sum_of_power_output") - col("stddev_power_output") * lit(2)) | (col("sum_power_output") > col("avg_of_sum_of_power_output") + col("stddev_power_output") * lit(2)))

  # Apply schema
  gold_df = apply_schema(gold_df, gold_schema, False)

  return gold_df
  