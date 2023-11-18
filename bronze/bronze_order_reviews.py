# Databricks notebook source
# MAGIC %run "../setup"

# COMMAND ----------

# MAGIC %run "./utils"

# COMMAND ----------

# MAGIC %run "./schemas"

# COMMAND ----------

bronze_order_reviews_df = normalize(spark.sql("SELECT * FROM jordan_project_3._airbyte_raw_order_reviews"), order_reviews_schema)

display(bronze_order_reviews_df)

# COMMAND ----------

ge_bronze_order_reviews_df = SparkDFDataset(bronze_order_reviews_df)

pk_not_null_expectation = ge_bronze_order_reviews_df.expect_column_values_to_not_be_null("review_id")

if not pk_not_null_expectation["success"]: 
    raise Exception(pk_not_null_expectation)

# COMMAND ----------

# Expectation to check the uniqueness of the combination of review_id and order_id
compound_columns_unique_expectation = ge_bronze_order_reviews_df.expect_compound_columns_to_be_unique(column_list=["review_id", "order_id"])

if not compound_columns_unique_expectation["success"]:
    raise ValueError(f"Duplicate entries found for the combination of review_id and order_id: {compound_columns_unique_expectation}")

# COMMAND ----------

from pyspark.sql.types import TimestampType

is_timestamp_expectation = ge_bronze_order_reviews_df.expect_column_values_to_be_of_type("updated_at", "TimestampType")

if not is_timestamp_expectation["success"]: 
    raise Exception(is_timestamp_expectation)

# COMMAND ----------

bronze_order_reviews_df.write.option("path", bronze_order_reviews_path).format("delta").mode("overwrite").saveAsTable(bronze_order_reviews_table_name)
