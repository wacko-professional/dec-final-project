# Databricks notebook source
# MAGIC %run "../setup"

# COMMAND ----------

# MAGIC %run "../silver/utils"

# COMMAND ----------

# MAGIC %run "./utils"

# COMMAND ----------

gold_sellers_df = spark.sql(f"""
    SELECT
      *
    FROM 
      {silver_sellers_table_name}
""")

display(gold_sellers_df)

# COMMAND ----------

gold_sellers_df.write.option("path", gold_sellers_path).format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(gold_sellers_table_name)
