# Databricks notebook source
# MAGIC %md
# MAGIC #Initialization

# COMMAND ----------


import pyspark.sql.functions as F
from pyspark.sql.types import StringType, DateType
from pyspark.sql.functions import col, trim
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read Bronze table

# COMMAND ----------

# DBTITLE 1,Cell 4
df = spark.table("dev_project.bronze.crm_prd_info")


# COMMAND ----------

# MAGIC %md
# MAGIC # Silver Transformationa

# COMMAND ----------

# MAGIC %md
# MAGIC ## Trimming

# COMMAND ----------

for field in df.schema.fields:
    if isinstance(field.dataType, StringType):
        df = df.withColumn(field.name, trim(col(field.name)))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Product Key Parsing

# COMMAND ----------

df = df.withColumn("cat_id", F.regexp_replace(F.substring(col("prd_key"), 1, 5), "-", "_"))
df = df.withColumn("prd_key", F.substring(col("prd_key"), 7, F.length(col("prd_key"))))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cost Cleanup

# COMMAND ----------

df = df.withColumn("prd_cost", F.coalesce(col("prd_cost"), F.lit(0)))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Product Line Normalization

# COMMAND ----------

df = (
    df
    # Normalize product line
    .withColumn(
        "prd_line",
        F.when(F.upper(col("prd_line")) == "M", "Mountain")
         .when(F.upper(col("prd_line")) == "R", "Road")
         .when(F.upper(col("prd_line")) == "S", "Other Sales")
         .when(F.upper(col("prd_line")) == "T", "Touring")
         .otherwise("n/a")
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Date Casting

# COMMAND ----------

df = df.withColumn("prd_start_dt", col("prd_start_dt").cast(DateType()))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Renaming Columns

# COMMAND ----------

RENAME_MAP = {
    "prd_id": "product_id",
    "cat_id": "category_id",
    "prd_key": "product_number",
    "prd_nm": "product_name",
    "prd_cost": "product_cost",
    "prd_line": "product_line",
    "prd_start_dt": "start_date",
    "prd_end_dt": "end_date"
}
for old_name, new_name in RENAME_MAP.items():
    df = df.withColumnRenamed(old_name, new_name)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sanity checks of dataframe

# COMMAND ----------

# DBTITLE 1,Cell 19
display(df.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC #Writing Silver Table

# COMMAND ----------

df.write.mode("overwrite").format("delta").saveAsTable("dev_project.silver.crm_products")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sanity checks of silver table

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM dev_project.silver.crm_products LIMIT 10

# COMMAND ----------

# MAGIC %md
# MAGIC