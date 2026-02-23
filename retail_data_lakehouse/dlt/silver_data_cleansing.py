from pipeline_config import *
import dlt
from datetime import datetime, timezone
from pyspark.sql import functions as F
from pyspark.sql.functions import broadcast

# --- SILVER: Cleaned Streams (Intermediate Views) ---

# Quality control for customers stream:
# - Warn if email is invalid or age/loyalty points are out of range, but only drop records missing customer_id.
# - Warn if gender is not recognized.
@dlt.view(name=CUSTOMERS_CLEANED_STREAM, comment="QC for customers stream")
@dlt.expect_or_drop("valid customer_id", "customer_id IS NOT NULL")
@dlt.expect("valid_email", "email IS NULL OR email LIKE '%@%.%'")
@dlt.expect("realistic_age", "age >= 16 AND age <= 100")
@dlt.expect("realistic_loyalty_points", "loyalty_points >= 0")
@dlt.expect("valid_gender", "gender IN ('Male', 'Female', 'Other')")
def customers_cleaned_stream():
    return dlt.readStream(BRONZE_CUSTOMERS)

# Quality control for products stream:
# - Warn if stock is negative or brand is missing.
# - Drop records missing product_id or with price not in [99-10000) range.
# - Drop records with NULL category to ensure data quality in gold tables.
@dlt.view(name=PRODUCTS_CLEANED_STREAM, comment="QC for products stream")
@dlt.expect_or_drop("valid_product_id", "product_id IS NOT NULL")
@dlt.expect_or_drop("valid_product_price", "price >= 99 AND price < 10000")
@dlt.expect_or_drop("valid_category", "category IS NOT NULL")
@dlt.expect("non_negative_stock", "stock_quantity >= 0")
@dlt.expect("valid_brand", "brand IS NOT NULL")
def products_cleaned_stream():
    return dlt.read_stream(BRONZE_PRODUCTS)

# Quality control for stores stream:
# - Replace missing manager names with 'Unknown' for reporting.
# - Warn if manager name is missing or status is invalid, but only drop records missing store_id.
@dlt.view(name=STORES_CLEANED_STREAM, comment="QC for stores stream")
@dlt.expect_or_drop("valid_store_id", "store_id IS NOT NULL")
@dlt.expect("valid_manager_name", "manager IS NOT NULL")
@dlt.expect("valid_status", "status IN ('Open', 'Under Renovation')")
def stores_cleaned_stream():
    return dlt.read_stream(BRONZE_STORES).withColumn("manager", F.coalesce(F.col("manager"), F.lit("Unknown")))

# Quality control for sales stream (fact table):
# - Warn if payment method is invalid or discount is negative.
# - Add a derived date column for time-based analytics.
# - Apply watermarking to handle late-arriving data.
# - Enforce referential integrity using LEFT SEMI joins (existence checks without bringing dimension columns).
#   This prevents orphaned records that would cause NULL values in gold layer.
@dlt.view(name=SALES_CLEANED_STREAM, comment="QC for sales stream (fact)")
@dlt.expect("valid_payment_method", "payment_method IN ('Cash', 'Credit Card', 'Debit Card', 'Mobile Pay', 'Gift Card', 'UPI', 'Net Banking')")
@dlt.expect("realistic_discount", "discount_applied >= 0")
def sales_cleaned_stream():
    sales = (
        dlt.read_stream(BRONZE_SALES)
        .withColumn("date", F.col("event_time").cast("date"))
        .withWatermark("event_time", "10 minutes")
        # Data delay is controlled by LATENCY_MAX_STREAMING in data generator
    )
    
    # Get current dimension tables for referential integrity checks
    # These are small lookup tables, so we use broadcast hints for optimal performance
    stores_current = dlt.read(f"live.{SILVER_STORES_CURRENT}")
    customers_current = dlt.read(f"live.{SILVER_CUSTOMERS_CURRENT}")
    products_current = dlt.read(f"live.{SILVER_PRODUCTS_CURRENT}")
    
    # Broadcast: Optimizes joins with small dimension tables (typically < 2GB)
    # This prevents orphaned records that would cause NULL values in gold layer
    return (
        sales
        .join(broadcast(stores_current.select("store_id").distinct()), "store_id", "left_semi")
        .join(broadcast(customers_current.select("customer_id").distinct()), "customer_id", "left_semi")
        .join(broadcast(products_current.select("product_id").distinct()), "product_id", "left_semi")
    )