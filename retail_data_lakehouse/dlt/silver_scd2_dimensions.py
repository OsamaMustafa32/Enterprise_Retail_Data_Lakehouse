from pipeline_config import *
import dlt
from datetime import datetime, timezone
from pyspark.sql import functions as F

# --- SILVER: Materialized SCD2 Business Dimension Tables ---
# (using create_auto_cdc_flow)

# Customer SCD2: Tracks changes to customer attributes over time for historical analysis and compliance.
# Ignores null updates to prevent accidental overwrites.
dlt.create_streaming_table(SILVER_CUSTOMERS)
dlt.create_auto_cdc_flow(
    target=SILVER_CUSTOMERS,
    source=f"live.{CUSTOMERS_CLEANED_STREAM}",
    keys=["customer_id"],
    sequence_by=F.col("last_update_time"),
    ignore_null_updates=True,
    track_history_column_list=['name', 'email', 'address', 'phone_number', 'gender'],
    except_column_list=["last_update_time", "ingest_timestamp", "source_file_path"],
    stored_as_scd_type=2
)

# Product SCD2: Tracks product attribute changes (pricing, branding, inventory, etc.) for trend analysis.
# Ignores null updates to avoid overwriting existing values.
dlt.create_streaming_table(SILVER_PRODUCTS)
dlt.create_auto_cdc_flow(
    target=SILVER_PRODUCTS,
    source=f"live.{PRODUCTS_CLEANED_STREAM}",
    keys=["product_id"],
    sequence_by=F.col("last_update_time"),
    ignore_null_updates=True,
    except_column_list=["last_update_time", "ingest_timestamp", "source_file_path"],
    track_history_column_list=['name', 'category', 'brand', 'price', 'description', 'color', 'size'],
    stored_as_scd_type=2
)

# Store SCD2: Tracks store attribute changes for operational and performance analysis.
# Ignores null updates to maintain data integrity.
dlt.create_streaming_table(SILVER_STORES)
dlt.create_auto_cdc_flow(
    target=SILVER_STORES,
    source=f"live.{STORES_CLEANED_STREAM}",
    keys=["store_id"],
    sequence_by=F.col("last_update_time"),
    ignore_null_updates=True,
    except_column_list=["last_update_time", "ingest_timestamp", "source_file_path"],
    track_history_column_list=['name', 'address', 'manager', 'status'],
    stored_as_scd_type=2
)