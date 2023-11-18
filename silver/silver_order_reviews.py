# Databricks notebook source
# MAGIC %run "../setup"

# COMMAND ----------

# MAGIC %run "../bronze/utils"

# COMMAND ----------

# MAGIC %run "../silver/utils"

# COMMAND ----------

# MAGIC %run "./utils"

# COMMAND ----------

silver_order_reviews_df = spark.sql(f"""
    SELECT
      *
    FROM 
      {bronze_order_reviews_table_name}
""")

display(silver_order_reviews_df)

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, col

key_columns = ['order_id', 'review_id']
windowSpec = Window.partitionBy(*key_columns).orderBy(col("updated_at").desc())

silver_order_reviews_df_with_row_number = silver_order_reviews_df.withColumn("row_num", row_number().over(windowSpec))

# keep only the most recent records for each key
deduped_silver_order_reviews_df = silver_order_reviews_df_with_row_number.filter(col("row_num") == 1).drop("row_num")

columns_to_drop = ['_airbyte_ab_id', '_airbyte_emitted_at']

cleaned_silver_order_reviews_df = silver_order_reviews_df.drop(*columns_to_drop)

display(silver_order_reviews_df_with_row_number)

# COMMAND ----------

cleaned_silver_order_reviews_df.write.option("path", silver_order_reviews_path).format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(silver_order_reviews_table_name)
