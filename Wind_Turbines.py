# Databricks notebook source
# import python helper functions
from pyspark.sql.functions import count, min, max, sum, when, coalesce, col, lit
from pyspark.sql.types import StructType, StructField, TimestampType, IntegerType, DecimalType
from functools import reduce
from operator import add

directory = "raw_data"

# COMMAND ----------

# get everything from directory
files = dbutils.fs.ls(directory)
# filter above with csv files only
csv_files = [x.name for x in files if x.name.endswith(".csv")]

list_of_dfs = []
for csv_file in csv_files:
    # read each file
    sourced = spark.read.csv(f"{directory}/{csv_file}", header=True, sep=",")
    # add to list of dfs
    list_of_dfs.append(sourced)
df = reduce(lambda a, b: a.union(b), list_of_dfs)

# define schema
Wind_Turbines_Schema = StructType(
    [
        StructField('timestamp', TimestampType()),
        StructField('turbine_id', IntegerType()),
        StructField('wind_speed', DecimalType(4, 2)),
        StructField('wind_direction', IntegerType()),
        StructField('power_output', DecimalType(4, 2))
        ]
    )

# apply schema
schema_columns = [col(x.name).cast(x.dataType).alias(x.name) if x.name in df.columns else lit(None).cast(x.dataType).alias(x.name) for x in Wind_Turbines_Schema.fields]
df = df.select(*schema_columns)

display(df)

# COMMAND ----------

# DQ checks
duplicate_rows = df.count() - df.dropDuplicates().count()
assert duplicate_rows == 0, f"{duplicate_rows} duplicate rows found"

min_wind_direction = df.agg(min("wind_direction")).collect()[0][0]
assert min_wind_direction >= 0, f"min_wind_direction should be at least 0, but is {min_wind_direction}"

max_wind_direction = df.agg(max("wind_direction")).collect()[0][0]
assert max_wind_direction < 360, f"max_wind_direction should be less than 360, but is {max_wind_direction}"

number_of_nulls = df.select(*[sum(col(c).isNull().cast("int")).alias(c) for c in df.columns]).withColumn("number_of_nulls", reduce(add, [col(x) for x in df.columns])).select(col("number_of_nulls")).collect()[0][0]
assert number_of_nulls == 0, f"not expecting any NULLs, but found {number_of_nulls}"

# COMMAND ----------

display(df.groupBy("turbine_id").agg(count("turbine_id").alias("counts"), min("wind_direction").alias("min_wind_direction"), max("wind_direction").alias("max_wind_direction")).orderBy("turbine_id"))
