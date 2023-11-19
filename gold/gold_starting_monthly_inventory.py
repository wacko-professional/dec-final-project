# Databricks notebook source
# MAGIC %run "../setup"

# COMMAND ----------

# MAGIC %run "../silver/utils"

# COMMAND ----------

# MAGIC %run "./utils"

# COMMAND ----------

from pyspark.sql.functions import col, rand, lit, explode, sequence, to_date, date_format
from datetime import datetime

# COMMAND ----------

# Define the range of dates
start_date = "2018-01-01"  # Adjust start date as needed
end_date = datetime.now().strftime("%Y-%m-%d")

# Generate a list of the first day of each month between the start and end dates
date_df = spark.sql(f"SELECT explode(sequence(to_date('{start_date}'), to_date('{end_date}'), interval 1 month)) as date")
date_df = date_df.withColumn("year", date_format(col("date"), "yyyy"))
date_df = date_df.withColumn("month", date_format(col("date"), "MM"))

# COMMAND ----------

silver_products_df = spark.read.table("silver_products")
display(silver_products_df)

# COMMAND ----------

gold_starting_inventory_df = silver_products_df.crossJoin(date_df).select(
    col('product_id'),
    col('year'),
    col('month'),
    (50 + (rand() * 100)).cast("int").alias('starting_inventory')  # Random int between 50 and 150
)
display(gold_starting_inventory_df)

# COMMAND ----------

gold_starting_inventory_df.write.option("path", gold_starting_inventory_path).format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(gold_starting_inventory_table_name)
