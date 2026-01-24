# Databricks notebook source
# MAGIC %md
# MAGIC #Select Catalog

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG dev_project;

# COMMAND ----------

# MAGIC %md
# MAGIC #Create Lakehouse Schemaa

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS bronze
# MAGIC COMMENT 'Bronze layer: raw ingested data';
# MAGIC
# MAGIC CREATE SCHEMA IF NOT EXISTS silver
# MAGIC COMMENT 'Silver layer: cleaned and transformed data';
# MAGIC
# MAGIC CREATE SCHEMA IF NOT EXISTS gold
# MAGIC COMMENT 'Gold layer: business-ready data';

# COMMAND ----------

# MAGIC %md
# MAGIC #Create Volume

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE VOLUME IF NOT EXISTS dev_project.bronze.source_systems
# MAGIC COMMENT 'Volume for raw source files (CSV)';