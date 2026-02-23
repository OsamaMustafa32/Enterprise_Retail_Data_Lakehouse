# Databricks notebook source
pip install faker

# COMMAND ----------

from pipeline_config import *

# COMMAND ----------

spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG_NAME}")
spark.sql(f"USE CATALOG {CATALOG_NAME}")

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {LANDING_SCHEMA}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {BRONZE_SCHEMA}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SILVER_SCHEMA}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {GOLD_SCHEMA}")

spark.sql(f"CREATE VOLUME IF NOT EXISTS {RAW_STREAMING_VOLUME}")

# COMMAND ----------

# Setting up file directories for raw data generator

for path in [RAW_SALES_PATH, RAW_PRODUCTS_PATH, RAW_STORES_PATH, RAW_CUSTOMERS_PATH]:
    try:
        dbutils.fs.ls(path)
        print("Path ", path, " already exists")
    except Exception:
        dbutils.fs.mkdirs(path)
        print("Created ", path)

# COMMAND ----------

