# Databricks notebook source
# MAGIC %md
# MAGIC #The Transformation Logic

# COMMAND ----------

query = """
SELECT
    sd.order_number,
    pr.product_key,
    cu.customer_key,
    sd.order_date,
    sd.ship_date,
    sd.due_date,
    sd.sales_amount,
    sd.quantity,
    sd.price
FROM dev_project.silver.crm_sales sd
LEFT JOIN dev_project.gold.dim_products pr
    ON sd.product_number = pr.product_number
LEFT JOIN dev_project.gold.dim_customers cu
    ON sd.customer_id = cu.customer_id;
"""
df = spark.sql(query)

# COMMAND ----------

# MAGIC %md
# MAGIC #Writing Gold Table

# COMMAND ----------

df.write.mode("overwrite").format("delta").saveAsTable("dev_project.gold.fact_sales")

# COMMAND ----------

# MAGIC %md
# MAGIC ##Sanity checks of Gold Table

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM workspace.gold.fact_sales LIMIT 10