# Databricks notebook source
# MAGIC %run "../setup"

# COMMAND ----------

# MAGIC %run "../silver/utils"

# COMMAND ----------

# MAGIC %run "./utils"

# COMMAND ----------

gold_products_df = spark.sql(f"""
    SELECT
      *
    FROM 
      {silver_products_table_name}
""")

display(gold_products_df)

# COMMAND ----------

gold_products_df.write.option("path", gold_products_path).format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(gold_products_table_name)
