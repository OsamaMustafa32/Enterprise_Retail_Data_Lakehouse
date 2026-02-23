from pipeline_config import *
import dlt
from datetime import datetime, timezone
from pyspark.sql import functions as F

# --- SILVER: Streaming Tables for Sales and Returns Transactions ---

# Streams sales transactions with positive quantities.
# Drops records with invalid discounts or missing foreign keys.
# Guarantees only valid sales transactions are available for analytics.
@dlt.table(name=SILVER_SALES_TRANSACTIONS, comment="Stream sales table (positive quantity)")
@dlt.expect_or_drop("valid_discount", "discount_applied >= 0")
@dlt.expect_or_drop("valid_store_id", "store_id IS NOT NULL")
@dlt.expect_or_drop("valid_customer_id", "customer_id IS NOT NULL")
@dlt.expect_or_drop("valid_product_id", "product_id IS NOT NULL")
def silver_sales_transactions():
    return (
        dlt.read_stream(f"live.{SALES_CLEANED_STREAM}")
        .filter(F.col("quantity") > 0)
    )

# Streams returns transactions with negative quantities.
# Converts negative values to absolute for reporting purposes.
# Drops records with invalid discounts or missing foreign keys.
# Ensures returns are tracked separately and accurately.
@dlt.table(name=SILVER_RETURNS_TRANSACTIONS, comment="Stream returns table (negative quantity)")
@dlt.expect_or_drop("valid_discount", "discount_applied >= 0")
@dlt.expect_or_drop("valid_store_id", "store_id IS NOT NULL")
@dlt.expect_or_drop("valid_customer_id", "customer_id IS NOT NULL")
@dlt.expect_or_drop("valid_product_id", "product_id IS NOT NULL")
def silver_returns_transactions():
    return (
        dlt.read_stream(f"live.{SALES_CLEANED_STREAM}")
        .filter(F.col("quantity") < 0)
        .withColumn("returned_quantity", F.abs(F.col("quantity")))
        .withColumn("returned_amount", F.abs(F.col("total_amount")))
        .drop("quantity", "total_amount")
    )