# Databricks notebook source
from pyspark.sql.functions import from_json, col

# COMMAND ----------

def normalize(df, schema):
    return (
        df.select(
            "_airbyte_ab_id",
            from_json(col("_airbyte_data"), schema).alias("data"),
            "_airbyte_emitted_at"
        )
        .select(
            "_airbyte_ab_id",
            "data.*",
            "_airbyte_emitted_at"
        )
    )

# COMMAND ----------

bronze_sellers_path = f"dbfs:/mnt/dbacademy-users/jordan_project_3/bronze/sellers"
bronze_sellers_table_name = f"bronze_sellers"

# COMMAND ----------

bronze_customers_path = f"dbfs:/mnt/dbacademy-users/jordan_project_3/bronze/customers"
bronze_customers_table_name = f"bronze_customers"

# COMMAND ----------

bronze_order_items_path = f"dbfs:/mnt/dbacademy-users/jordan_project_3/bronze/order_items"
bronze_order_items_table_name = f"bronze_order_items"

# COMMAND ----------

bronze_order_payments_path = f"dbfs:/mnt/dbacademy-users/jordan_project_3/bronze/order_payments"
bronze_order_payments_table_name = f"bronze_order_payments"

# COMMAND ----------

bronze_order_reviews_path = f"dbfs:/mnt/dbacademy-users/jordan_project_3/bronze/order_reviews"
bronze_order_reviews_table_name = f"bronze_order_reviews"

# COMMAND ----------

bronze_orders_path = f"dbfs:/mnt/dbacademy-users/jordan_project_3/bronze/orders"
bronze_orders_table_name = f"bronze_orders"

# COMMAND ----------

bronze_products_path = f"dbfs:/mnt/dbacademy-users/jordan_project_3/bronze/products"
bronze_products_table_name = f"bronze_products"
