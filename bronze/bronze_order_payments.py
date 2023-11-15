# Databricks notebook source
# MAGIC %run "../setup"

# COMMAND ----------

# MAGIC %run "./utils"

# COMMAND ----------

# MAGIC %run "./schemas"

# COMMAND ----------

display(spark.sql("SELECT * FROM jordan_project_3._airbyte_raw_order_payments"))

# COMMAND ----------

bronze_order_payments_df = normalize(spark.sql("SELECT * FROM jordan_project_3._airbyte_raw_order_payments"), order_payments_schema)

display(bronze_order_payments_df)


# COMMAND ----------

from pyspark.sql.types import TimestampType

ge_bronze_order_payments_df = SparkDFDataset(bronze_order_payments_df)

is_timestamp_expectation = ge_bronze_order_payments_df.expect_column_values_to_be_of_type("updated_at", "TimestampType")

if not is_timestamp_expectation["success"]: 
    raise Exception(is_timestamp_expectation)

# COMMAND ----------

bronze_order_payments_df.write.option("path", bronze_order_payments_path).format("delta").mode("overwrite").saveAsTable(bronze_order_payments_table_name)
