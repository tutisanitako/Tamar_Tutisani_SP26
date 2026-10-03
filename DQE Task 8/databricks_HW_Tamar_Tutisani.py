# Databricks notebook source
# MAGIC %md
# MAGIC ## Databricks Homework
# MAGIC Since in July 2025 Databricks Community Edition was deprecated and instead of creating separate cluster they are being provided in serverless mode it will be easier for you to work with data - since all the data and tables will be saving not only when cluster as active.
# MAGIC
# MAGIC So, no separate activities for cluser creating should be executed - it will be autoattached/started when you will execute any of the cells below.
# MAGIC
# MAGIC
# MAGIC Please, create table in the default schema using file Sales_December_2019.csv. On the left found Catalog => Add Data => Drop files to upload, or click to browse => Sales_December_2019.csv After file will be uploaded, just need to confirm that table should be uploaded.
# MAGIC
# MAGIC  Make sure that the first row is header selected => Create Table. Table will be created with name that you specified (sales_december_2019 by default) You will be able to change the table name later if needed.

# COMMAND ----------

# MAGIC %md
# MAGIC PySpark can process SQL queries as a text. In other words you don't need to switch cell language to SQL.
# MAGIC 1. Write data from table that you created into the dataframe using PySpark with SQL query. Show data in the dataframe

# COMMAND ----------

# Create DataFrame from SQL query
df = spark.sql("SELECT * FROM workspace.default.sales_december_2019")
df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC Any notebook can be parameterized using dbutils.widgets. Try to add one parameter "Product_name" and select data from dataframe filtered by value from this parameter. 
# MAGIC
# MAGIC 2. Select data where product = "product_name" from dataframe using PySpark

# COMMAND ----------

dbutils.widgets.text("Product_name", "USB-C Charging Cable")
product_name = dbutils.widgets.get("Product_name")

df_filtered = df.filter(df["Product"] == product_name)
df_filtered.show()

# COMMAND ----------

# MAGIC %md
# MAGIC As well as in SQL, in PySpark you can use aggregate functions. Package pyspark.sql.functions contains all aggregated function from SQL. Try to perform simple aggregation with dataframe. Don't forget, that column types, which you want to calculate, shoud be numerical.  
# MAGIC 3. Calculate the sales for each product, including the number of products sold

# COMMAND ----------

from pyspark.sql import functions as F

sales_by_product = (
    df
    .filter(F.col("Quantity Ordered") != "Quantity Ordered")  # Filter out header rows
    .withColumn("Quantity Ordered", F.col("Quantity Ordered").cast("int"))
    .withColumn("Price Each", F.col("Price Each").cast("double"))
    .groupBy("Product")
    .agg(
        F.sum(F.col("Quantity Ordered") * F.col("Price Each")).alias("Total_Sales"),
        F.sum("Quantity Ordered").alias("Total_Quantity_Sold")
    )
    .orderBy(F.col("Total_Sales").desc())
)

sales_by_product.show(20, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC In the PySpark you can perform dataframe profiling using one of two special commands or simple aggregated functions. Try to find special commands to complete this task or just use aggregated functions. Hint: please, сhange the column data types based on the data in them
# MAGIC
# MAGIC 4. Show data profiles output for the new dataframe of table sales_december_2019_csv: row count, min and max value for each column

# COMMAND ----------

df_typed = (
    df
    .filter(F.col("Quantity Ordered") != "Quantity Ordered")  # Filter out header rows
    .withColumn("Quantity Ordered", F.col("Quantity Ordered").cast("int"))
    .withColumn("Price Each", F.col("Price Each").cast("double"))
    .withColumn("Order Date", F.to_timestamp(F.col("Order Date"), "MM/dd/yy HH:mm"))
)

profiling_sales = df_typed.agg(
    F.count("*").alias("Row_Count"),
    F.count("Quantity Ordered").alias("Qty_count"),
    F.count("Price Each").alias("Price_count"),
    F.count("Order Date").alias("Date_count"),
    F.count("Order ID").alias("ID_count"),
    F.count("Product").alias("Product_count"),
    F.count("Purchase Address").alias("Address_count"),
    F.min("Quantity Ordered").alias("Min_Qty_Ordered"),
    F.max("Quantity Ordered").alias("Max_Qty_Ordered"),
    F.min("Price Each").alias("Min_Price"),
    F.max("Price Each").alias("Max_Price"),
    F.min("Order Date").alias("Min_Date"),
    F.max("Order Date").alias("Max_Date")
)

profiling_sales.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC 5. Add new column to the dataframe from previous task with any default value that you want

# COMMAND ----------

df_with_flag = df_typed.withColumn("Data_Source", F.lit("Sales_December_2019.csv"))
df_with_flag.show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC Temporary views are processed by cluster and always dropped when the session ends (when the cluster turns off).
# MAGIC
# MAGIC 6. Create temporary view from task 4 dataframe using PySpark and perform any select using SQL

# COMMAND ----------

# code for view creation
df_with_flag.createOrReplaceTempView("sales_december_2019_profiled")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT Product, COUNT(*) AS Orders_Count, ROUND(AVG(`Price Each`), 2) AS Avg_Price
# MAGIC FROM sales_december_2019_profiled
# MAGIC GROUP BY Product
# MAGIC ORDER BY Orders_Count DESC