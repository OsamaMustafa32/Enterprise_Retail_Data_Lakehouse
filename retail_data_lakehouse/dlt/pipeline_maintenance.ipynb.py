# Databricks notebook source
from pipeline_config import *

# COMMAND ----------

print("--- Starting to drop tables ---")
for schema in [BRONZE_SCHEMA, SILVER_SCHEMA, GOLD_SCHEMA]:
  full_schema_name = f"{CATALOG_NAME}.{schema}"
  try:
    print(f"\nProcessing schema: {full_schema_name}")
    # Get all tables in the current catalog and schema
    tables_df = spark.sql(f"SHOW TABLES IN {full_schema_name}")
    print(f"  Found {tables_df.count()} tables in {full_schema_name}")
    # Collect table names to avoid issues with modifying the list while iterating
    tables_to_check = [row.tableName for row in tables_df.collect()]

    for table_name in tables_to_check:
      full_table_name = f"{full_schema_name}.{table_name}"
      # Check if the table name does not start with 'bridge'
      try:
          spark.sql(f"DROP TABLE {full_table_name}")
          print(f"  SUCCESS: Dropped {full_table_name}")
          # Check and delete checkpoint directory
          checkpoint_path = f"/checkpoint/{full_table_name.replace('.', '/')}"
          try:
              dbutils.fs.ls(checkpoint_path)
              dbutils.fs.rm(checkpoint_path, recurse=True)
              print(f"  SUCCESS: Deleted checkpoint directory {checkpoint_path}")
          except Exception:
              print(f"  INFO: No checkpoint directory found for {checkpoint_path}")
      except Exception as e:
          print(f"  FAILED to drop {full_table_name}: {e}")

  except Exception as e:
    print(f"Could not process schema '{full_schema_name}': {e}")

print("\n--- Deletion process complete ---")

# COMMAND ----------

for path in [RAW_SALES_PATH, RAW_CUSTOMERS_PATH, RAW_PRODUCTS_PATH, RAW_STORES_PATH]:
    dbutils.fs.rm(path, recurse=True)

# COMMAND ----------

checkpoint_path = "/checkpoint/apparel_store/01_bronze/01_bronze"
try:
    files = dbutils.fs.ls(checkpoint_path)
    print(f"Checkpoint directory exists: {checkpoint_path}")
    for file in files:
        print(f"  {file.name}")
except Exception:
    print(f"No checkpoint directory found at {checkpoint_path}")