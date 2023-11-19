# Databricks notebook source
gold_starting_inventory_path = f"dbfs:/mnt/dbacademy-users/jordan_project_3/gold/starting_inventory"
gold_starting_inventory_table_name = f"gold_starting_inventory"

# COMMAND ----------

gold_lead_time_path = f"dbfs:/mnt/dbacademy-users/jordan_project_3/gold/lead_time"
gold_lead_time_table_name = f"fact_lead_time"

# COMMAND ----------

gold_customers_path = f"dbfs:/mnt/dbacademy-users/jordan_project_3/gold/customers"
gold_customers_table_name = f"customers_dim"

# COMMAND ----------

gold_products_path = f"dbfs:/mnt/dbacademy-users/jordan_project_3/gold/products"
gold_products_table_name = f"products_dim"

# COMMAND ----------

gold_sellers_path = f"dbfs:/mnt/dbacademy-users/jordan_project_3/gold/sellers"
gold_sellers_table_name = f"sellers_dim"

# COMMAND ----------

gold_order_items_path = f"dbfs:/mnt/dbacademy-users/jordan_project_3/gold/order_items"
gold_order_items_table_name = f"fact_order_items"
