# Databricks notebook source
# MAGIC %run "../setup"

# COMMAND ----------

# MAGIC %run "./utils"

# COMMAND ----------

# MAGIC %run "./schemas"

# COMMAND ----------

bronze_products_df = normalize(spark.sql("SELECT * FROM jordan_project_3._airbyte_raw_products"), products_schema)

display(bronze_products_df)

# COMMAND ----------

ge_bronze_products_df = SparkDFDataset(bronze_products_df)

pk_not_null_expectation = ge_bronze_products_df.expect_column_values_to_not_be_null("product_id")

if not pk_not_null_expectation["success"]: 
    raise Exception(pk_not_null_expectation)

# COMMAND ----------

has_timestamp_expectation = ge_bronze_products_df.expect_column_to_exist("updated_at")

if not has_timestamp_expectation["success"]: 
    raise Exception(has_timestamp_expectation)

# COMMAND ----------

from pyspark.sql.types import TimestampType

is_timestamp_expectation = ge_bronze_products_df.expect_column_values_to_be_of_type("updated_at", "TimestampType")

if not is_timestamp_expectation["success"]: 
    raise Exception(is_timestamp_expectation)

# COMMAND ----------

bronze_products_df.write.option("path", bronze_products_path).format("delta").mode("overwrite").saveAsTable(bronze_products_table_name)
