-- agg_delivery_performance.sql
-- Delivery performance by customer state: on-time rate, average delivery days.

{{ config(materialized='table') }}

select
    customer_state,
    count(distinct order_id)                     as total_orders,
    avg(delivery_days)                           as avg_delivery_days,
    avg(estimated_delivery_days)                 as avg_estimated_delivery_days,
    avg(delivery_days - estimated_delivery_days) as avg_delay_days,
    countif(is_on_time = true)                   as on_time_deliveries,
    safe_divide(
        countif(is_on_time = true),
        count(distinct order_id)
    )                                            as on_time_rate,
    avg(review_score)                            as avg_review_score

from {{ ref('fct_orders') }}
where customer_state is not null
  and delivery_days is not null
group by customer_state
order by total_orders desc
