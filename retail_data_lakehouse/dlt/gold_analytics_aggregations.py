from pipeline_config import *
import dlt
from datetime import datetime, timezone
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# -------------------- GOLD LAYER: Analytics & Aggregates --------------------

# This table joins sales transactions with the latest customer, product, and store dimension tables.
# Columns are aliased for clarity and usability in BI tools.
@dlt.table(
    name=GOLD_DENORMALIZED_SALES_FACTS,
    comment="Fully denormalized sales fact table for analytic use. A streaming table."
)
def denormalized_sales_facts():
    sales = dlt.read_stream(f"LIVE.{SILVER_SALES_TRANSACTIONS}")
    customers = dlt.read(f"LIVE.{SILVER_CUSTOMERS_CURRENT}")
    products = dlt.read(f"LIVE.{SILVER_PRODUCTS_CURRENT}")
    stores = dlt.read(f"LIVE.{SILVER_STORES_CURRENT}")
    
    # Use INNER joins for all dimensions since silver layer enforces referential integrity
    # Silver layer filters out sales with invalid foreign keys, so all joins can be INNER
    # This ensures no NULL values in store_name, customer_name, or product_name
    df = (
        sales
        .join(stores, "store_id", "inner")  # INNER: Silver ensures store exists
        .join(customers, "customer_id", "inner")  # INNER: Silver ensures customer exists
        .join(products, "product_id", "inner")  # INNER: Silver ensures product exists
        .select(
            sales.transaction_id,
            sales.event_time,
            sales.customer_id,
            customers.name.alias("customer_name"),
            sales.product_id,
            products.name.alias("product_name"),
            products.category.alias("product_category"),
            sales.store_id,
            stores.name.alias("store_name"),
            sales.quantity,
            sales.unit_price,
            sales.total_amount,
            sales.payment_method,
            sales.discount_applied,
            sales.tax_amount,
        )
    )
    return df

# This table aggregates sales by store and day.
# It calculates total revenue, transaction count, items sold, and unique customers for each store per day.
# Useful for daily performance tracking and store-level analytics.
@dlt.table(
    name=GOLD_DAILY_SALES_BY_STORE,
    comment="Daily store sales summary. A batch table."
)
def gold_daily_sales_by_store():
    df = dlt.read(f"LIVE.{GOLD_DENORMALIZED_SALES_FACTS}")
    agg = (
        df.groupBy(
                F.window("event_time", "1 day").alias("sale_window"),
                "store_id",
                "store_name"
            )
            .agg(
                F.round(F.sum("total_amount"), 2).alias("total_revenue"),
                F.count("transaction_id").alias("total_transactions"),
                F.sum("quantity").alias("total_items_sold"),
                F.countDistinct("customer_id").alias("unique_customers")
            )
            .select(
                F.col("sale_window.start").cast("date").alias("sale_date"),
                "store_id",
                "store_name",
                "total_revenue",
                "total_transactions",
                "total_items_sold",
                "unique_customers"
            )
    )
    return agg

# This table tracks product performance metrics.
# It helps merchandising, inventory, and marketing teams understand which products drive sales.
@dlt.table(
    name=GOLD_PRODUCT_PERFORMANCE,
    comment="Product sales performance metrics. A batch table."
)
def gold_product_performance():
    df = dlt.read(f"LIVE.{GOLD_DENORMALIZED_SALES_FACTS}")
    agg = (
        df.groupBy("product_id", "product_name", "product_category")
          .agg(
              F.round(F.sum("total_amount"),2).alias("total_revenue"),
              F.round(F.sum("quantity"), 2).alias("total_quantity_sold"),
              F.count("transaction_id").alias("total_orders")
          )
    )
    return agg

# This table calculates customer lifetime value and RFM segmentation.
# RFM (Recency, Frequency, Monetary) scores help marketing teams target customers based on their purchase behavior.
@dlt.table(
    name=GOLD_CUSTOMER_LIFETIME_VALUE,
    comment="Customer lifetime value metrics with RFM segmentation. A batch table."
)
def gold_customer_lifetime_value():    
    df = dlt.read(f"LIVE.{GOLD_DENORMALIZED_SALES_FACTS}")
    agg = (
        df.groupBy("customer_id", "customer_name")
          .agg(
              F.round(F.sum(F.coalesce(F.col("total_amount"), F.lit(0))), 2).alias("total_spend"),
              F.countDistinct("transaction_id").alias("total_orders"),
              F.min("event_time").cast("date").alias("first_purchase_date"),
              F.max("event_time").cast("date").alias("last_purchase_date"),
              F.round(F.avg(F.coalesce(F.col("total_amount"), F.lit(0))), 2).alias("avg_order_value")
          )
          .filter(
              (F.col("total_orders") > 0) & 
              (F.col("total_spend") >= 0) &
              (F.col("last_purchase_date").isNotNull())
          )
          .withColumn(
              "customer_name",
              F.coalesce(F.col("customer_name"), F.lit("Unknown"))
          )
          .withColumn(
              "days_since_last_purchase",
              F.datediff(F.current_date(), F.col("last_purchase_date"))
          )
          .filter(F.col("days_since_last_purchase") >= 0)
    )
    
    # Calculate RFM quintiles using ntile window function
    recency_window = Window.orderBy(F.col("days_since_last_purchase").asc())
    frequency_window = Window.orderBy(F.col("total_orders").desc())
    monetary_window = Window.orderBy(F.col("total_spend").desc())
    
    agg = (
        agg.withColumn("r_score", F.ntile(5).over(recency_window))
           .withColumn("f_score", F.ntile(5).over(frequency_window))
           .withColumn("m_score", F.ntile(5).over(monetary_window))
           .withColumn("rfm_score", F.concat(
               F.col("r_score").cast("string"),
               F.col("f_score").cast("string"),
               F.col("m_score").cast("string")
           ))
           .withColumn(
               "rfm_segment",
               F.when(
                   (F.col("r_score") >= 4) & (F.col("f_score") >= 4) & (F.col("m_score") >= 4),
                   "Champions"
               ).when(
                   (F.col("r_score") >= 3) & (F.col("f_score") >= 3) & (F.col("m_score") >= 3),
                   "Loyal Customers"
               ).when(
                   (F.col("r_score") <= 2) & ((F.col("f_score") >= 3) | (F.col("m_score") >= 3)),
                   "At Risk"
               ).when(
                   (F.col("r_score") >= 4) & ((F.col("f_score") <= 2) | (F.col("m_score") <= 2)),
                   "New/Potential"
               ).otherwise("Lost")
           )
    )
    
    return agg.select(
        "customer_id",
        "customer_name",
        "total_spend",
        "total_orders",
        "avg_order_value",
        "first_purchase_date",
        "last_purchase_date",
        "days_since_last_purchase",
        "r_score",
        "f_score",
        "m_score",
        "rfm_score",
        "rfm_segment"
    )