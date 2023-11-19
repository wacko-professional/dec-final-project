# Databricks notebook source
# MAGIC %run "../setup"

# COMMAND ----------

# MAGIC %run "../silver/utils"

# COMMAND ----------

# MAGIC %run "./utils"

# COMMAND ----------

gold_order_items_df = spark.sql(f"""
    SELECT
      *
    FROM 
      {silver_order_items_table_name}
""")

display(gold_order_items_df)

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import sum as _sum, count

# Group by order_id and product_id, then aggregate
fact_order_items_df = (gold_order_items_df.groupBy("order_id", "product_id", "seller_id")
                 .agg(
                     _sum("price").alias("total_price"),
                     count("order_item_id").alias("quantity_purchased")
                 ))
display(fact_order_items_df)

# COMMAND ----------

fact_order_items_df.write.option("path", gold_order_items_path).format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(gold_order_items_table_name)
