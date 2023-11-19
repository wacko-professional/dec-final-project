# Databricks notebook source
# Accumulating Fact

# COMMAND ----------

# MAGIC %run "../setup"

# COMMAND ----------

# MAGIC %run "./utils"

# COMMAND ----------

orders_df = spark.read.table("jordan_project_3.silver_orders")

# COMMAND ----------

from pyspark.sql.functions import when, col, datediff, lit, to_date

lead_time_df = orders_df.withColumn("time_to_approval", 
                   when(col("order_approved_at").isNotNull(),
                        datediff(to_date(col("order_approved_at")), to_date(col("order_purchase_timestamp"))))
                   .otherwise(lit(None)))\
                .withColumn("time_to_carrier", 
                   when(col("order_delivered_carrier_date").isNotNull(),
                        datediff(to_date(col("order_delivered_carrier_date")), to_date(col("order_approved_at"))))
                   .otherwise(lit(None)))\
                .withColumn("time_to_delivery", 
                   when(col("order_delivered_customer_date").isNotNull(),
                        datediff(to_date(col("order_delivered_customer_date")), to_date(col("order_delivered_carrier_date"))))
                   .otherwise(lit(None)))

# COMMAND ----------

lead_time_df.write.option("path", gold_lead_time_path).format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(gold_lead_time_table_name)
