# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %sql
# MAGIC use catalog superstore_catalog;
# MAGIC use schema gold_schema;

# COMMAND ----------

df_gold = spark.table("superstore_catalog.silver_schema.silver_table_external")

# COMMAND ----------

df_gold.display()

# COMMAND ----------

df_aggr_data = (
    df_gold.groupBy("Region","Category","Order_Year")
    .agg(
        expr("sum(Sales) as Total_Sales"),
        expr("sum(Profit) as Total_Profit"),
        expr("avg(Discount) as Avg_Discount"),
        expr("avg(Quantity) as Avg_Quantity"),
        expr("sum(Quantity) as Total_Quantity")
        ).orderBy("Region","Category","Order_Year")
)

# COMMAND ----------

df_aggr_data.display()

# COMMAND ----------

df_aggr_data.write.format('delta')\
            .mode('overwrite')\
            .option("overwriteSchema", "true")\
            .saveAsTable('superstore_catalog.gold_schema.gold_aggr_table')

# COMMAND ----------

# MAGIC %sql
# MAGIC describe extended superstore_catalog.gold_schema.gold_aggr_table;

# COMMAND ----------

# MAGIC %md
# MAGIC ### SHALLOW CLONE

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE superstore_catalog.gold_schema.gold_table_shallow_clone
# MAGIC shallow clone superstore_catalog.silver_schema.silver_table_external;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE superstore_catalog.gold_schema.gold_table_shallow_clone
# MAGIC shallow clone superstore_catalog.silver_schema.silver_table;

# COMMAND ----------

# MAGIC %md
# MAGIC ### -- DEEP CLONE

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE superstore_catalog.gold_schema.gold_table_deep_clone
# MAGIC deep clone superstore_catalog.silver_schema.silver_table_external;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE superstore_catalog.gold_schema.gold_table_deep_clone2
# MAGIC deep clone superstore_catalog.silver_schema.silver_table;

# COMMAND ----------

# MAGIC %md
# MAGIC ### OPTIMIZATION TECHNIQUES

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE superstore_catalog.gold_schema.gold_table_deep_clone;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from superstore_catalog.gold_schema.gold_table_deep_clone 
# MAGIC where Region = 'WEST' AND Order_Year = '2017';

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE superstore_catalog.gold_schema.gold_table_deep_clone
# MAGIC ZORDER BY (Region, Order_Year);

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from superstore_catalog.gold_schema.gold_table_deep_clone 
# MAGIC where Region = 'WEST' AND Order_Year = '2017';

# COMMAND ----------

# MAGIC %md
# MAGIC AUTO COMPACT & AUTO OPTIMIZE

# COMMAND ----------

# MAGIC %sql
# MAGIC alter table superstore_catalog.gold_schema.gold_table_deep_clone 
# MAGIC set tblproperties (
# MAGIC   'delta.autoOptimize.optimizeWrite' = 'true',
# MAGIC   'delta.autoOptimize.autoCompact' = 'true'
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC show tblproperties superstore_catalog.gold_schema.gold_table_deep_clone;

# COMMAND ----------

df_gold.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", True) \
    .saveAsTable("superstore_catalog.gold_schema.gold_table_managed")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC -- LIQUID CLUSTERING

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE superstore_catalog.gold_schema.gold_table_cluster
# MAGIC cluster by(Region, Order_Year)
# MAGIC as
# MAGIC select * from superstore_catalog.gold_schema.gold_table_deep_clone;

# COMMAND ----------

# MAGIC %sql
# MAGIC describe detail superstore_catalog.gold_schema.gold_table_cluster;

# COMMAND ----------

# MAGIC %md
# MAGIC --CACHING

# COMMAND ----------

spark.catalog.cacheTable("superstore_catalog.gold_schema.gold_table_cluster")

# COMMAND ----------

# MAGIC %md
# MAGIC -- Data Skipping

# COMMAND ----------

# MAGIC %sql
# MAGIC SET spark.databricks.io.skipping.mandatory = true;

# COMMAND ----------

# MAGIC %sql
# MAGIC describe detail superstore_catalog.gold_schema.gold_table_cluster;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY superstore_catalog.gold_schema.gold_table_cluster;

# COMMAND ----------

# MAGIC %md
# MAGIC --PARTITIONING

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS superstore_catalog.gold_schema.gold_table_partitioned
# MAGIC PARTITIONED BY (Region, Order_Year)
# MAGIC AS
# MAGIC SELECT * FROM superstore_catalog.gold_schema.gold_table_deep_clone;

# COMMAND ----------

# MAGIC %sql
# MAGIC describe detail superstore_catalog.gold_schema.gold_table_partitioned;

# COMMAND ----------

# MAGIC %md
# MAGIC ### CDF

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE superstore_catalog.gold_schema.gold_table_partitioned
# MAGIC SET TBLPROPERTIES (
# MAGIC   'delta.enableChangeDataFeed' = 'true'
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC describe history superstore_catalog.gold_schema.gold_table_partitioned;

# COMMAND ----------

# MAGIC %sql
# MAGIC update superstore_catalog.gold_schema.gold_table_partitioned
# MAGIC set Category = 'Furniture'
# MAGIC where Region = 'SOUTH'

# COMMAND ----------

# MAGIC %sql
# MAGIC describe history superstore_catalog.gold_schema.gold_table_partitioned;

# COMMAND ----------

df = spark.read.format('delta')\
          .option('readChangeFeed', 'true')\
          .option('startingVersion', 1)\
           .option('endingVersion', 2)\
          .table("superstore_catalog.gold_schema.gold_table_partitioned")

df.display()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from table_changes(
# MAGIC   'superstore_catalog.gold_schema.gold_table_partitioned',  
# MAGIC   '2025-07-22 18:28:04',
# MAGIC   '2025-07-22 18:30:17'
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC -- DELETION VECTORS

# COMMAND ----------

# MAGIC %sql
# MAGIC describe detail superstore_catalog.gold_schema.gold_table_partitioned;

# COMMAND ----------

# MAGIC %sql
# MAGIC alter table superstore_catalog.gold_schema.gold_table_partitioned
# MAGIC set tblproperties (
# MAGIC   'delta.enableDeletionVectors' = 'true'
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC -- VACUUM - DRY RUN

# COMMAND ----------

# MAGIC %sql
# MAGIC VACUUM superstore_catalog.gold_schema.gold_table_partitioned RETAIN 10 HOURS;

# COMMAND ----------

# MAGIC %sql
# MAGIC SET spark.databricks.delta.retentionDurationCheck.enabled = false;

# COMMAND ----------

# MAGIC %sql
# MAGIC VACUUM superstore_catalog.gold_schema.gold_table_partitioned RETAIN 10 HOURS DRY RUN;

# COMMAND ----------

# MAGIC %md
# MAGIC -- DATA GOVERNANCE & LINEAGE

# COMMAND ----------

# MAGIC %md
# MAGIC RLS

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace temp view gold_table_view as (
# MAGIC   select * from superstore_catalog.gold_schema.gold_table_partitioned
# MAGIC   where Region = 'east'
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC CLS

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM superstore_catalog.gold_schema.gold_table_partitioned;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW superstore_catalog.gold_schema.gold_table_masked AS
# MAGIC SELECT
# MAGIC   Row_ID,
# MAGIC   Order_ID,
# MAGIC   Order_Date,
# MAGIC   Ship_Date,
# MAGIC   Ship_Mode,
# MAGIC   Customer_ID,
# MAGIC   Customer_Name,
# MAGIC   Segment,
# MAGIC   Country,
# MAGIC   City,
# MAGIC   State,
# MAGIC   Postal_Code,
# MAGIC   Region,
# MAGIC   Product_ID,
# MAGIC   Category,
# MAGIC   Sub_Category,
# MAGIC   Product_Name,
# MAGIC   Sales,
# MAGIC   Quantity,
# MAGIC   Discount,
# MAGIC   Profit,
# MAGIC   Country_Code,
# MAGIC   Order_Year,
# MAGIC   Order_Number,
# MAGIC   Product_Category,
# MAGIC   Product_SubCategory,
# MAGIC   '****' AS product_number_fullmask,
# MAGIC   CONCAT(SUBSTRING(Product_Number, 1, 3), '****') AS product_number_partialmask,
# MAGIC   sha2(Product_Number, 256) AS product_number_hashmask
# MAGIC FROM superstore_catalog.gold_schema.gold_table_partitioned;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from superstore_catalog.gold_schema.gold_table_masked limit 100;

# COMMAND ----------

# MAGIC %md
# MAGIC -- TAGS

# COMMAND ----------

# MAGIC %sql
# MAGIC alter table superstore_catalog.gold_schema.gold_table_partitioned set tags ('sensitivity' = 'PII');

# COMMAND ----------

# MAGIC %md
# MAGIC -- DATA LINEAGE

# COMMAND ----------

