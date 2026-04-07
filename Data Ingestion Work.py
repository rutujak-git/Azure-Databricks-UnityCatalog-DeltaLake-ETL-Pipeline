# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %sql
# MAGIC use catalog superstore_catalog;
# MAGIC use schema bronze_schema;

# COMMAND ----------

df_bronze = spark.read.format('csv')\
                .option('header', 'true')\
                .option('inferSchema', 'true')\
                .load('abfss://superstoredata@adlsprojectsuperstore.dfs.core.windows.net/bronze/superstore.csv')

df_bronze.display()               

# COMMAND ----------

def write_to_bronze_table(df, table_name):
    df.write.format('delta')\
            .mode('overwrite')\
            .option('overwriteSchema', 'true')\
            .option('delta.columnMapping.mode', 'name')\
            .saveAsTable(f"superstore_catalog.bronze_schema.{table_name}")

write_to_bronze_table(df_bronze, "bronze_table")

# COMMAND ----------

