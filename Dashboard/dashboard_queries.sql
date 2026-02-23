-- =========================================
-- Executive Summary Page
-- =========================================

-- 1. Total Revenue
-- Purpose: Calculates the total revenue for the selected date range.
-- Visualization: Counter "Total Revenue"
SELECT ROUND(SUM(total_amount), 2) AS total_revenue
FROM apparel_store.03_gold.gold_denormalized_sales_facts
WHERE event_time >= :date_range.min
  AND event_time <= :date_range.max;

-- 2. Total Transactions
-- Purpose: Counts the total number of unique transactions in the selected date range.
-- Visualization: Counter "Total Transactions"
SELECT COUNT(DISTINCT transaction_id) AS total_transactions
FROM apparel_store.03_gold.gold_denormalized_sales_facts
WHERE event_time >= :date_range.min
  AND event_time <= :date_range.max;

-- 3. Total Customers
-- Purpose: Counts the total number of unique customers in the selected date range.
-- Visualization: Counter "Total Customers"
SELECT COUNT(DISTINCT customer_id) AS total_customers
FROM apparel_store.03_gold.gold_denormalized_sales_facts
WHERE event_time >= :date_range.min
  AND event_time <= :date_range.max;

-- 4. Average Order Value (30 Days)
-- Purpose: Calculates the average order value for the selected date range.
-- Visualization: Counter "Average Order Value"
SELECT ROUND(AVG(total_amount), 2) AS avg_order_value
FROM apparel_store.03_gold.gold_denormalized_sales_facts
WHERE event_time >= :date_range.min
  AND event_time <= :date_range.max;

-- 5. Revenue Trend - All Stores
-- Purpose: Shows the daily total revenue trend across all stores.
-- Visualization: Line chart "Revenue Trend - Overall (All Stores)"
SELECT sale_date,
       ROUND(SUM(total_revenue), 2) AS daily_total_revenue
FROM apparel_store.03_gold.gold_daily_sales_by_store
WHERE sale_date >= :date_range.min
  AND sale_date <= :date_range.max
GROUP BY sale_date
ORDER BY sale_date ASC;

-- 6. Top 5 Stores by Revenue
-- Purpose: Lists the top five stores by total revenue in the selected date range.
-- Visualization: Bar chart "Top 5 Stores by Revenue"
SELECT store_name,
       ROUND(SUM(total_revenue), 2) AS total_revenue
FROM apparel_store.03_gold.gold_daily_sales_by_store
WHERE sale_date >= :date_range.min
  AND sale_date <= :date_range.max
GROUP BY store_name
ORDER BY total_revenue DESC
LIMIT 5;

-- 7. Top 5 Products by Revenue
-- Purpose: Lists the top five products by total revenue in the selected date range.
-- Visualization: Bar chart "Top 5 Products by Revenue"
SELECT product_name,
       product_category,
       ROUND(SUM(total_amount), 2) AS total_revenue
FROM apparel_store.03_gold.gold_denormalized_sales_facts
WHERE event_time >= :date_range.min
  AND event_time <= :date_range.max
GROUP BY product_name,
         product_category
ORDER BY total_revenue DESC
LIMIT 5;

-- 8. Revenue by Product Category (Date Filterable)
-- Purpose: Aggregates total revenue by product category for the selected date range.
-- Visualization: Bar chart "Revenue by Category"
SELECT product_category,
       ROUND(SUM(total_amount), 2) AS total_revenue
FROM apparel_store.03_gold.gold_denormalized_sales_facts
WHERE event_time >= :date_range.min
  AND event_time <= :date_range.max
GROUP BY product_category
ORDER BY total_revenue DESC;

-- 9. Revenue by Payment Method
-- Purpose: Aggregates total revenue by payment method for the selected date range.
-- Visualization: Pie chart "Revenue by Payment Method"
SELECT payment_method,
       ROUND(SUM(total_amount), 2) AS total_revenue
FROM apparel_store.03_gold.gold_denormalized_sales_facts
WHERE event_time >= :date_range.min
  AND event_time <= :date_range.max
GROUP BY payment_method
ORDER BY total_revenue DESC;

-- =========================================
-- Product Analytics Page
-- =========================================

-- 10. Category Performance Summary
-- Purpose: Summarizes product category performance including product count, revenue, quantity, and average price.
-- Visualization: Table "Category Performance Summary", Pie "Product Sales Distribution by Category", Line "Correlation: Product Price vs Quantity Sold"
SELECT product_category,
       COUNT(DISTINCT product_id) AS product_count,
       ROUND(SUM(total_amount), 2) AS total_revenue,
       SUM(quantity) AS total_quantity,
       ROUND(SUM(total_amount) / SUM(quantity), 2) AS avg_price
FROM apparel_store.03_gold.gold_denormalized_sales_facts
WHERE event_time >= :date_range.min
  AND event_time <= :date_range.max
GROUP BY product_category
ORDER BY total_revenue DESC;

-- 11. Top 20 Products
-- Purpose: Lists the top 20 products by revenue, including category, quantity sold, and order count.
-- Visualization: Table "Top 20 Products by Revenue", Bar "Product Sales Distribution by Quantity Sold"
SELECT product_name,
       product_category,
       ROUND(SUM(total_amount), 2) AS total_revenue,
       SUM(quantity) AS total_quantity_sold,
       COUNT(transaction_id) AS total_orders
FROM apparel_store.03_gold.gold_denormalized_sales_facts
WHERE event_time >= :date_range.min
  AND event_time <= :date_range.max
GROUP BY product_name,
         product_category
ORDER BY total_revenue DESC
LIMIT 20;

-- =========================================
-- Store Performance Page
-- =========================================

-- 12. Store Performance Summary
-- Purpose: Summarizes store-level performance metrics including revenue, transactions, unique customers, and derived KPIs.
-- Visualization: Table "Store Performance"
SELECT store_name,
       ROUND(SUM(total_revenue), 2) AS total_revenue,
       SUM(total_transactions) AS total_transactions,
       SUM(unique_customers) AS total_unique_customers,
       ROUND(SUM(total_revenue) / SUM(total_transactions), 2) AS avg_transaction_value,
       ROUND(SUM(total_items_sold) / SUM(total_transactions), 2) AS items_per_transaction,
       ROUND(SUM(total_transactions) / SUM(unique_customers), 2) AS transactions_per_customer
FROM apparel_store.03_gold.gold_daily_sales_by_store
WHERE sale_date >= :date_range.min
  AND sale_date <= :date_range.max
GROUP BY store_name
ORDER BY total_revenue DESC;

-- 13. Bottom 5 Stores by Revenue
-- Purpose: Identifies the five stores with the lowest total revenue in the selected date range.
-- Visualization: Bar chart "Bottom 5 Stores (Need Attention)"
SELECT store_name,
       sale_date,
       ROUND(SUM(total_revenue), 2) AS total_revenue
FROM apparel_store.03_gold.gold_daily_sales_by_store
WHERE sale_date >= :date_range.min
  AND sale_date <= :date_range.max
GROUP BY store_name,
         sale_date
ORDER BY total_revenue ASC
LIMIT 5;

-- 14. Active vs Inactive Stores (Date Filterable)
-- Purpose: Counts active and inactive stores based on sales activity in the selected date range.
-- Visualization: Pie chart "Active vs Inactive Stores"
SELECT CASE
           WHEN sf.store_id IS NOT NULL THEN 'Active'
           ELSE 'Inactive'
       END AS store_status,
       COUNT(DISTINCT st.store_id) AS store_count
FROM apparel_store.02_silver.silver_stores st
LEFT JOIN (
    SELECT DISTINCT store_id
    FROM apparel_store.03_gold.gold_denormalized_sales_facts
    WHERE event_time >= :date_range.min
      AND event_time <= :date_range.max
) sf ON st.store_id = sf.store_id
GROUP BY store_status;

-- 15. Customer Engagement Index in Top 10 Stores
-- Purpose: Calculates engagement index and related metrics for the top 10 stores by revenue.
-- Visualization: Line chart "Customer Engagement Index (Top 10 Stores)"
SELECT store_name,
       ROUND(SUM(total_revenue), 2) AS total_revenue,
       SUM(total_transactions) AS total_transactions,
       SUM(unique_customers) AS total_unique_customers,
       ROUND(SUM(total_revenue) / SUM(total_transactions), 2) AS avg_transaction_value,
       ROUND(SUM(total_items_sold) / SUM(total_transactions), 2) AS items_per_transaction,
       ROUND(SUM(total_transactions) / SUM(unique_customers), 2) AS transactions_per_customer,
       items_per_transaction * transactions_per_customer AS customer_engagement_Index
FROM apparel_store.03_gold.gold_daily_sales_by_store
WHERE sale_date >= :date_range.min
  AND sale_date <= :date_range.max
GROUP BY store_name
ORDER BY total_revenue DESC
LIMIT 10;

-- =========================================
-- Customer Analytics Page
-- =========================================

-- 16. Customer Segmentation (RFM)
-- Purpose: Segments customers by RFM (Recency, Frequency, Monetary) and counts customers in each segment.
-- Visualization: Pie chart "Customer Segmentation (RFM)"
SELECT rfm_segment,
       COUNT(*) AS customer_count
FROM apparel_store.03_gold.gold_customer_lifetime_value
GROUP BY rfm_segment
ORDER BY CASE rfm_segment
             WHEN 'Champions' THEN 1
             WHEN 'Loyal Customers' THEN 2
             WHEN 'At Risk' THEN 3
             WHEN 'New/Potential' THEN 4
             WHEN 'Lost' THEN 5
         END;

-- 17. RFM Segment Performance
-- Purpose: Summarizes performance metrics for each RFM segment.
-- Visualization: Table "RFM Segment Performance", Bar "Average Order Value by RFM Segment"
SELECT rfm_segment,
       COUNT(*) AS customer_count,
       ROUND(SUM(total_spend), 2) AS total_revenue,
       ROUND(AVG(total_spend), 2) AS avg_spend,
       ROUND(AVG(total_orders), 2) AS avg_orders,
       ROUND(AVG(avg_order_value), 2) AS avg_order_value,
       ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS percentage
FROM apparel_store.03_gold.gold_customer_lifetime_value
GROUP BY rfm_segment
ORDER BY CASE rfm_segment
             WHEN 'Champions' THEN 1
             WHEN 'Loyal Customers' THEN 2
             WHEN 'At Risk' THEN 3
             WHEN 'New/Potential' THEN 4
             WHEN 'Lost' THEN 5
         END;

-- 18. Customer Acquisition Trend (Last 90 Days)
-- Purpose: Tracks the number of new customers acquired each day over the last 90 days.
-- Visualization: Line chart "Customer Acquisition Trend (Last 90 Days)"
SELECT DATE(first_purchase_date) AS acquisition_date,
       COUNT(*) AS new_customers
FROM apparel_store.03_gold.gold_customer_lifetime_value
WHERE first_purchase_date >= CURRENT_DATE - INTERVAL 90 DAYS
GROUP BY DATE(first_purchase_date)
ORDER BY acquisition_date ASC;

-- 19. Top 100 Customers by Lifetime Value
-- Purpose: Lists the top 100 customers by total spend, including order and RFM details.
-- Visualization: Table "Top 100 Customers by Lifetime Value"
SELECT customer_id,
       customer_name,
       ROUND(total_spend, 2) AS total_spend,
       total_orders,
       ROUND(avg_order_value, 2) AS avg_order_value,
       first_purchase_date,
       last_purchase_date,
       rfm_segment
FROM apparel_store.03_gold.gold_customer_lifetime_value
ORDER BY total_spend DESC
LIMIT 100;