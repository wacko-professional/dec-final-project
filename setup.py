# Databricks notebook source
## TODO:
## Use this example: https://github.com/Data-Engineer-Camp/2023-07-bootcamp/tree/main/08-databricks-spark/3/01-ins-databricks-workflows/solved
## To create a setup

# COMMAND ----------

spark.sql("USE jordan_project_3")

# COMMAND ----------

# MAGIC %pip install great_expectations

# COMMAND ----------

from great_expectations.dataset import SparkDFDataset
