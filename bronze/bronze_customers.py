# Databricks notebook source
# MAGIC %run "../setup"

# COMMAND ----------

# MAGIC %run "./utils"

# COMMAND ----------

# MAGIC %run "./schemas"

# COMMAND ----------

bronze_customers_df = normalize(spark.sql("SELECT * FROM jordan_project_3.`_airbyte_raw_customers`"), customer_schema)

display(bronze_customers_df)

# COMMAND ----------

ge_bronze_customers_df = SparkDFDataset(bronze_customers_df)

pk_not_null_expectation = ge_bronze_customers_df.expect_column_values_to_not_be_null("customer_id")

if not pk_not_null_expectation["success"]: 
    raise Exception(pk_not_null_expectation)

# COMMAND ----------

has_timestamp_expectation = ge_bronze_customers_df.expect_column_to_exist("updated_at")

if not has_timestamp_expectation["success"]: 
    raise Exception(has_timestamp_expectation)

# COMMAND ----------

bronze_customers_df.write.option("path", bronze_customers_path).format("delta").mode("overwrite").saveAsTable(bronze_customers_table_name)
