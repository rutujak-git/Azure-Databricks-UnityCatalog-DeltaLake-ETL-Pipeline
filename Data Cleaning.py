# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

df_silver = spark.table("superstore_catalog.bronze_schema.bronze_table")

df_silver.display()

# COMMAND ----------

df_silver.printSchema()

# COMMAND ----------

df_silver = df_silver.withColumn('Postal Code', col('Postal Code').cast('string'))
df_silver.display()

# COMMAND ----------

df_silver = df_silver.dropDuplicates()

# COMMAND ----------

df_silver.display()

# COMMAND ----------

df_silver = df_silver.dropna(subset=["Order ID","Customer ID","Order Date","Sales"])
df_silver.display()

# COMMAND ----------

df_silver = df_silver.filter(
    col('Sales').cast('double').isNotNull() &
    col('Quantity').cast('double').isNotNull() &
    col('Discount').cast('double').isNotNull() &
    col('Profit').cast('double').isNotNull()
)
df_silver.display()

# COMMAND ----------

df_silver.printSchema()

# COMMAND ----------

df_silver = df_silver.withColumn('Sales', col('Sales').cast('double'))
df_silver = df_silver.withColumn('Quantity', col('Quantity').cast('int'))
df_silver = df_silver.withColumn('Discount', col('Discount').cast('double'))
df_silver = df_silver.withColumn('Profit', col('Profit').cast('double'))

df_silver.printSchema()

# COMMAND ----------

df_silver = df_silver.filter(
    (col("Sales") >= 0) & 
    (col("Quantity") >= 0) &
    (col("Discount") >= 0) &
    (col("Profit") >= 0)
)
df_silver.display()

# COMMAND ----------

str_cols = ['Ship Mode','Customer Name','Segment','Country','City','State','Region','Postal Code','Category','Sub-Category','Product Name']

for c in str_cols:
    df_silver = df_silver.withColumn(c, trim(upper(col(c))))
    
df_silver.display()


# COMMAND ----------

df_silver = df_silver.filter(col('Ship Date') > col('Order Date'))
df_silver.display()

# COMMAND ----------

from pyspark.sql.functions import col

# List of columns you want to rename
columns_to_rename = [
    "Row ID","Order ID", "Order Date", "Ship Date", "Ship Mode", 
    "Customer ID", "Customer Name", "Postal Code", 
    "Product ID", "Sub-Category", "Product Name"
]

# Rename columns one by one
for old_name in columns_to_rename:
    new_name = old_name.replace(" ", "_").replace("-", "_")
    if old_name in df_silver.columns:
        df_silver = df_silver.withColumnRenamed(old_name, new_name)

df_silver.display()

# COMMAND ----------

df_silver = df_silver.withColumn("Country_Code",split(col("Order_ID"), "-").getItem(0))\
                     .withColumn("Order_Year",split(col("Order_ID"), "-").getItem(1))\
                     .withColumn("Order_Number",split(col("Order_ID"), "-").getItem(2))

df_silver.display()

# COMMAND ----------

df_silver= df_silver.withColumn("Product_Category",split(col("Product_ID"),"-").getItem(0))\
                   .withColumn("Product_SubCategory",split(col("Product_ID"),"-").getItem(1))\
                   .withColumn("Product_Number",split(col("Product_ID"),"-").getItem(2))

df_silver.display()

# COMMAND ----------

df_silver = df_silver.na.drop()
df_silver.display()

# COMMAND ----------

df_silver.write.format('delta')\
         .mode('overwrite')\
         .option('mergeSchema', 'true')\
         .saveAsTable(f"superstore_catalog.silver_schema.silver_table")

# COMMAND ----------

# MAGIC %sql
# MAGIC describe extended superstore_catalog.silver_schema.silver_table;

# COMMAND ----------

df_silver.write.format('delta')\
         .mode('overwrite')\
         .option('mergeSchema', 'true')\
         .option("path","abfss://superstoredata@adlsprojectsuperstore.dfs.core.windows.net/silver/silver_table_managed")\
         .save()

# COMMAND ----------

# MAGIC %sql
# MAGIC describe extended delta.`abfss://superstoredata@adlsprojectsuperstore.dfs.core.windows.net/silver/silver_table_managed`;

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists superstore_catalog.silver_schema.silver_table_external
# MAGIC using delta
# MAGIC location 'abfss://superstoredata@adlsprojectsuperstore.dfs.core.windows.net/silver/silver_table_external'
# MAGIC as select * from superstore_catalog.silver_schema.silver_table;
# MAGIC     

# COMMAND ----------

# MAGIC %sql
# MAGIC describe extended superstore_catalog.silver_schema.silver_table_external;

# COMMAND ----------

# MAGIC %md
# MAGIC ### DML Commands 

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct Discount from superstore_catalog.silver_schema.silver_table_external
# MAGIC where Region = 'WEST';

# COMMAND ----------

# MAGIC %sql
# MAGIC update superstore_catalog.silver_schema.silver_table_external
# MAGIC set Discount = 0.15
# MAGIC where Region = 'WEST';

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from superstore_catalog.silver_schema.silver_table_external ;

# COMMAND ----------

# MAGIC %sql
# MAGIC delete from superstore_catalog.silver_schema.silver_table_external
# MAGIC where Quantity=1;

# COMMAND ----------

# MAGIC %md
# MAGIC ### MERGE

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE superstore_catalog.silver_schema.silver_table_merge (
# MAGIC   Row_ID INT,
# MAGIC   Order_ID STRING,
# MAGIC   Order_Date DATE,
# MAGIC   Ship_Date DATE,
# MAGIC   Ship_Mode STRING,
# MAGIC   Customer_ID STRING,
# MAGIC   Customer_Name STRING,
# MAGIC   Segment STRING,
# MAGIC   Country STRING,
# MAGIC   City STRING,
# MAGIC   State STRING,
# MAGIC   Postal_Code STRING,
# MAGIC   Region STRING,
# MAGIC   Product_ID STRING,
# MAGIC   Category STRING,
# MAGIC   Sub_Category STRING,
# MAGIC   Product_Name STRING,
# MAGIC   Sales DOUBLE,
# MAGIC   Quantity INT,
# MAGIC   Discount DOUBLE,
# MAGIC   Profit DOUBLE,
# MAGIC   Country_Code STRING,
# MAGIC   Order_Year STRING,
# MAGIC   Order_Number STRING,
# MAGIC   Product_Category STRING,
# MAGIC   Product_SubCategory STRING,
# MAGIC   Product_Number STRING
# MAGIC )
# MAGIC USING DELTA;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO superstore_catalog.silver_schema.silver_table_merge VALUES
# MAGIC (9997, 'CA-2025-111003', DATE('2014-06-01'), DATE('2014-06-06'), 'Standard Class', 'CR-12625', 'Corey Roper', 'Home Office',
# MAGIC  'United States', 'Lakewood', 'New Jersey', '8701', 'East', 'OFF-BI-10001072', 'Office Supplies', 'Binders', 'GBC Clear Cover', 45.48, 3, 0.5, 777.77, 'CA', '2014', '111003', 'OFF', 'BI', '10001072'),
# MAGIC (9998, 'CA-2025-110984', DATE('2014-06-05'), DATE('2014-06-10'), 'Second Class', 'CR-12626', 'Jane Smith', 'Corporate',
# MAGIC  'United States', 'Denver', 'Colorado', '80202', 'West', 'OFF-EN-10001069', 'Office Supplies', 'Envelopes', 'Staples Envelopes', 23.95, 2, 0.1, 50.00, 'US', '2014', '110984', 'OFF', 'EN', '10001069');

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO superstore_catalog.silver_schema.silver_table_external AS target
# MAGIC USING superstore_catalog.silver_schema.silver_table_merge AS source
# MAGIC ON target.Order_ID = source.Order_ID
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET
# MAGIC     target.Row_ID = source.Row_ID,
# MAGIC     target.Order_Date = source.Order_Date,
# MAGIC     target.Ship_Date = source.Ship_Date,
# MAGIC     target.Ship_Mode = source.Ship_Mode,
# MAGIC     target.Customer_ID = source.Customer_ID,
# MAGIC     target.Customer_Name = source.Customer_Name,
# MAGIC     target.Segment = source.Segment,
# MAGIC     target.Country = source.Country,
# MAGIC     target.City = source.City,
# MAGIC     target.State = source.State,
# MAGIC     target.Postal_Code = source.Postal_Code,
# MAGIC     target.Region = source.Region,
# MAGIC     target.Product_ID = source.Product_ID,
# MAGIC     target.Category = source.Category,
# MAGIC     target.Sub_Category = source.Sub_Category,
# MAGIC     target.Product_Name = source.Product_Name,
# MAGIC     target.Sales = source.Sales,
# MAGIC     target.Quantity = source.Quantity,
# MAGIC     target.Discount = source.Discount,
# MAGIC     target.Profit = source.Profit,
# MAGIC     target.Country_Code = source.Country_Code,
# MAGIC     target.Order_Year = source.Order_Year,
# MAGIC     target.Order_Number = source.Order_Number,
# MAGIC     target.Product_Category = source.Product_Category,
# MAGIC     target.Product_SubCategory = source.Product_SubCategory,
# MAGIC     target.Product_Number = source.Product_Number
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT (
# MAGIC     Row_ID, Order_ID, Order_Date, Ship_Date, Ship_Mode, Customer_ID, Customer_Name,
# MAGIC     Segment, Country, City, State, Postal_Code, Region, Product_ID, Category,
# MAGIC     Sub_Category, Product_Name, Sales, Quantity, Discount, Profit,
# MAGIC     Country_Code, Order_Year, Order_Number, Product_Category, Product_SubCategory, Product_Number
# MAGIC   )
# MAGIC   VALUES (
# MAGIC     source.Row_ID, source.Order_ID, source.Order_Date, source.Ship_Date, source.Ship_Mode, source.Customer_ID, source.Customer_Name,
# MAGIC     source.Segment, source.Country, source.City, source.State, source.Postal_Code, source.Region, source.Product_ID, source.Category,
# MAGIC     source.Sub_Category, source.Product_Name, source.Sales, source.Quantity, source.Discount, source.Profit,
# MAGIC     source.Country_Code, source.Order_Year, source.Order_Number, source.Product_Category, source.Product_SubCategory, source.Product_Number
# MAGIC   );
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM superstore_catalog.silver_schema.silver_table_external
# MAGIC WHERE Order_ID IN ('CA-2025-111003', 'CA-2025-110984');

# COMMAND ----------

# MAGIC %md
# MAGIC ### --VERSION HISTORY & TIME TRAVEL

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY superstore_catalog.silver_schema.silver_table_external;

# COMMAND ----------

# MAGIC %sql
# MAGIC RESTORE superstore_catalog.silver_schema.silver_table_external TO VERSION AS OF 0;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM superstore_catalog.silver_schema.silver_table_external;

# COMMAND ----------

