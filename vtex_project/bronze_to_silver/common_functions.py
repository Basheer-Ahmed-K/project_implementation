# Databricks notebook source
import re
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType
from delta.tables import DeltaTable

# COMMAND ----------

def rename_columns_to_snake_case(df):
    for column in df.columns:
        res = ""
        for i in column:
            if i.isupper():
                res += "_" + i.lower()
            else:
                res += i
        df = df.withColumnRenamed(column, res.lstrip("_"))
    return df

# COMMAND ----------

def write_to_table(df,mode,table_name):
  df.write.format("delta").mode(mode).saveAsTable(table_name)

# COMMAND ----------

def save_to_delta_with_merge(df, path, database_name, target_table_name, merge_col):
    mapped_col = " AND ".join(list(map((lambda x: f"old.{x} = new.{x} "),merge_col))) # if we have multiple PK
    if not DeltaTable.isDeltaTable(spark,f"{path}"):
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {database_name}")
        df.write.mode("overwrite").format("delta").saveAsTable(f"{database_name}.{target_table_name}")
    else:
        deltaTable = DeltaTable.forPath(spark, f"{path}")
        deltaTable.alias("old").merge(df.alias("new"),mapped_col)\
        .whenMatchedUpdateAll()\
        .whenNotMatchedInsertAll()\
        .execute()