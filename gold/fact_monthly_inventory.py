# Databricks notebook source
# Periodic Snapshot: End of Month Inventory

# COMMAND ----------

# Load the orders and order_items datasets
orders_df = spark.read.table("jordan_project_3.silver_orders")
order_items_df = spark.read.table("jordan_project_3.silver_order_items")

# COMMAND ----------

display(orders_df)

# COMMAND ----------

display(order_items_df)

# COMMAND ----------

# Join orders and order_items to calculate monthly sales per product
from pyspark.sql.functions import col, sum, year, month, count

monthly_sales_df = orders_df.join(order_items_df, orders_df.order_id == order_items_df.order_id) \
    .groupBy(
        order_items_df.product_id,
        year(orders_df.order_purchase_timestamp).alias('year'),
        month(orders_df.order_purchase_timestamp).alias('month')
    ) \
    .agg(count('order_item_id').alias('sold_inventory')) \
    .withColumn("year", col("year").cast("int")) \
    .withColumn("month", col("month").cast("int"))

#monthly_sales_df.createOrReplaceTempView("monthly_sales")
display(monthly_sales_df)

# COMMAND ----------

# Dummy Starting Inventory Data
starting_inventory_df = spark.read.table("gold_starting_inventory")
starting_inventory_df = starting_inventory_df.withColumn("month", col("month").cast("int"))
starting_inventory_df = starting_inventory_df.withColumn("year", col("year").cast("int"))
display(starting_inventory_df)

# COMMAND ----------

monthly_sales_df.createOrReplaceTempView("monthly_sales")
starting_inventory_df.createOrReplaceTempView("starting_inventory")

# COMMAND ----------

spark.sql(
    """
CREATE OR REPLACE TABLE jordan_project_3.fact_inventory_monthly
USING DELTA
AS
SELECT 
    COALESCE(ms.year, si.year) AS year,
    COALESCE(ms.month, si.month) AS month,
    COALESCE(ms.product_id, si.product_id) AS product_id,
    COALESCE(si.starting_inventory, 0) AS starting_inventory,
    COALESCE(ms.sold_inventory, 0) AS sold_inventory,
    (COALESCE(si.starting_inventory, 0) - COALESCE(ms.sold_inventory, 0)) AS ending_inventory
FROM 
    monthly_sales ms
FULL OUTER JOIN 
    starting_inventory si 
ON 
    ms.product_id = si.product_id AND ms.year = si.year AND ms.month = si.month
"""
)


# COMMAND ----------

display(spark.sql("""
          SELECT * FROM fact_inventory_monthly
          """))
