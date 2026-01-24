# Databricks notebook source
# MAGIC %md
# MAGIC #The Transformation Logic

# COMMAND ----------

# DBTITLE 1,Cell 2
query = """
SELECT
    ROW_NUMBER() OVER (ORDER BY ci.customer_id) AS customer_key,
    ci.customer_id,
    ci.customer_number,
    ci.first_name,
    ci.last_name,
    la.country,
    ci.marital_status,
    CASE
        WHEN ci.gender <> 'n/a' THEN ci.gender
        ELSE COALESCE(ca.gender, 'n/a')
    END AS gender,
    ca.birth_date AS birthdate,
    ci.created_date AS create_date
FROM dev_project.silver.crm_customers ci
LEFT JOIN dev_project.silver.erp_customers ca
    ON ci.customer_number = ca.customer_number
LEFT JOIN dev_project.silver.erp_customer_location la
    ON ci.customer_number = la.customer_number
"""
df = spark.sql(query)

# COMMAND ----------

df.limit(10).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #Writing Gold Table

# COMMAND ----------

df.write.mode("overwrite").format("delta").saveAsTable("dev_project.gold.dim_customers")

# COMMAND ----------

# MAGIC %md
# MAGIC ##Sanity checks of Gold table

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM dev_project.gold.dim_customers LIMIT 10