# Databricks notebook source
# MAGIC %run "../setup"

# COMMAND ----------

# MAGIC %run "./utils"

# COMMAND ----------

# MAGIC %run "./schemas"

# COMMAND ----------

bronze_sellers_df = normalize(spark.sql("SELECT * FROM jordan_project_3._airbyte_raw_sellers"), sellers_schema)

display(bronze_sellers_df)

# COMMAND ----------

ge_bronze_sellers_df = SparkDFDataset(bronze_sellers_df)

pk_not_null_expectation = ge_bronze_sellers_df.expect_column_values_to_not_be_null("seller_id")

if not pk_not_null_expectation["success"]: 
    raise Exception(pk_not_null_expectation)

# COMMAND ----------

pk_unique_expectation = ge_bronze_sellers_df.expect_column_values_to_be_unique("seller_id")

if not pk_unique_expectation["success"]: 
    raise Exception(pk_unique_expectation)

# COMMAND ----------

has_timestamp_expectation = ge_bronze_sellers_df.expect_column_to_exist("updated_at")

if not has_timestamp_expectation["success"]: 
    raise Exception(has_timestamp_expectation)

# COMMAND ----------

from pyspark.sql.types import TimestampType

is_timestamp_expectation = ge_bronze_sellers_df.expect_column_values_to_be_of_type("updated_at", "TimestampType")

if not is_timestamp_expectation["success"]: 
    raise Exception(is_timestamp_expectation)

# COMMAND ----------


bronze_sellers_df.write.option("path", bronze_sellers_path).format("delta").mode("overwrite").saveAsTable(bronze_sellers_table_name)
