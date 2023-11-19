# Databricks notebook source
# MAGIC %run "../setup"

# COMMAND ----------

# MAGIC %run "../silver/utils"

# COMMAND ----------

# MAGIC %run "./utils"

# COMMAND ----------

gold_customers_df = spark.sql(f"""
    SELECT
      *
    FROM 
      {silver_customers_table_name}
""")

display(gold_customers_df)

# COMMAND ----------

gold_customers_df.write.option("path", gold_customers_path).format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(gold_customers_table_name)
