-- agg_monthly_orders.sql
-- Monthly order volume, revenue, and delivery performance trend (2016–2018).
-- PRIMARY DASHBOARD TILE: temporal line/bar chart showing growth over time.

{{ config(materialized='table') }}

select
    order_purchase_year,
    order_purchase_month,
    format_date('%Y-%m', order_purchase_date)   as year_month,
    count(distinct order_id)                    as total_orders,
    count(*)                                    as total_order_items,
    sum(total_item_value)                       as total_revenue,
    avg(total_item_value)                       as avg_order_item_value,
    avg(review_score)                           as avg_review_score,
    avg(delivery_days)                          as avg_delivery_days,
    countif(is_on_time = true)                  as on_time_deliveries,
    safe_divide(
        countif(is_on_time = true),
        count(distinct order_id)
    )                                           as on_time_rate

from {{ ref('fct_orders') }}
group by order_purchase_year, order_purchase_month, year_month
order by order_purchase_year, order_purchase_month
