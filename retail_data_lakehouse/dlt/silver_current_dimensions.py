from pipeline_config import *
import dlt
from datetime import datetime, timezone
from pyspark.sql import functions as F

# --- SILVER: Create "Current" Dimension Views ---
#
# These views filter the SCD2 dimension tables to return only the current records
# (where __END_AT is null). This provides the latest snapshot for analytics and
# lookup purposes, ensuring that downstream queries always reference the most
# up-to-date attributes for customers, products, and stores.

# Current customers: Only records with no end date (active SCD2 rows)
@dlt.view(name=SILVER_CUSTOMERS_CURRENT, comment="Current customers (SCD2)")
def silver_customers_current():
    return dlt.read(SILVER_CUSTOMERS).filter(F.col("__END_AT").isNull())

# Current products: Latest product attributes (active SCD2 rows)
@dlt.view(name=SILVER_PRODUCTS_CURRENT, comment="Current products (SCD2)")
def silver_products_current():
    return dlt.read(SILVER_PRODUCTS).filter(F.col("__END_AT").isNull())

# Current stores: Latest store attributes (active SCD2 rows)
@dlt.view(name=SILVER_STORES_CURRENT, comment="Current stores (SCD2)")
def silver_stores_current():
    return dlt.read(SILVER_STORES).filter(F.col("__END_AT").isNull())