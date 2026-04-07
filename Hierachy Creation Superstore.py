# Databricks notebook source
# MAGIC %md
# MAGIC ### 1. Create Credentials

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Create 3 External Locations

# COMMAND ----------

# MAGIC %sql
# MAGIC create external location if not exists bronze_ext_loc
# MAGIC url 'abfss://superstoredata@adlsprojectsuperstore.dfs.core.windows.net/bronze'
# MAGIC with (storage credential superstore_cred);
# MAGIC
# MAGIC create external location if not exists silver_ext_loc
# MAGIC url 'abfss://superstoredata@adlsprojectsuperstore.dfs.core.windows.net/silver'
# MAGIC with (storage credential superstore_cred);
# MAGIC
# MAGIC create external location if not exists gold_ext_loc
# MAGIC url 'abfss://superstoredata@adlsprojectsuperstore.dfs.core.windows.net/gold'
# MAGIC with (storage credential superstore_cred);

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. create catalog

# COMMAND ----------

# MAGIC %sql
# MAGIC create catalog if not exists superstore_catalog;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. Create Schemas

# COMMAND ----------

# MAGIC %sql
# MAGIC create schema if not exists superstore_catalog.bronze_schema;
# MAGIC create schema if not exists superstore_catalog.silver_schema;
# MAGIC create schema if not exists superstore_catalog.gold_schema;

# COMMAND ----------

