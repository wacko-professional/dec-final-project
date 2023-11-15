# Databricks notebook source
# MAGIC %run "../setup"

# COMMAND ----------

# MAGIC %run "./utils"

# COMMAND ----------

# MAGIC %run "./schemas"

# COMMAND ----------

display(spark.sql("SELECT * FROM jordan_project_3._airbyte_raw_order_items"))

# COMMAND ----------

# NOTE: We can have multiple order_id, product_id, seller_id / duplicates and need to aggregate them
bronze_order_items_df = normalize(spark.sql("SELECT * FROM jordan_project_3._airbyte_raw_order_items"), order_items_schema)

display(bronze_order_items_df)

# COMMAND ----------

ge_bronze_order_items_df = SparkDFDataset(bronze_order_items_df)

has_timestamp_expectation = ge_bronze_order_items_df.expect_column_to_exist("updated_at")

if not has_timestamp_expectation["success"]: 
    raise Exception(has_timestamp_expectation)

# COMMAND ----------

bronze_order_items_df.write.option("path", bronze_order_items_path).format("delta").mode("overwrite").saveAsTable(bronze_order_items_table_name)
