-- stg_orders.sql
-- Staging model: cleaned and typed Olist orders from the BigQuery raw layer.
-- Grain: one row per order item (matches orders_enriched table from Spark).

{{ config(materialized='view') }}

with source as (

    select * from {{ source('olist_raw', 'orders_enriched') }}

),

cleaned as (

    select
        cast(order_id                       as string)    as order_id,
        cast(order_item_id                  as int64)     as order_item_id,
        cast(order_status                   as string)    as order_status,
        cast(order_purchase_timestamp       as timestamp) as order_purchase_timestamp,
        cast(order_purchase_date            as date)      as order_purchase_date,
        cast(order_purchase_year            as int64)     as order_purchase_year,
        cast(order_purchase_month           as int64)     as order_purchase_month,
        cast(order_delivered_customer_date  as timestamp) as order_delivered_customer_date,
        cast(order_estimated_delivery_date  as timestamp) as order_estimated_delivery_date,
        cast(delivery_days                  as float64)   as delivery_days,
        cast(estimated_delivery_days        as float64)   as estimated_delivery_days,
        cast(is_on_time                     as bool)      as is_on_time,
        cast(customer_id                    as string)    as customer_id,
        cast(customer_unique_id             as string)    as customer_unique_id,
        cast(customer_city                  as string)    as customer_city,
        cast(customer_state                 as string)    as customer_state,
        cast(product_id                     as string)    as product_id,
        cast(product_category_name          as string)    as product_category_name,
        cast(product_category_name_english  as string)    as product_category_name_english,
        cast(seller_id                      as string)    as seller_id,
        cast(seller_city                    as string)    as seller_city,
        cast(seller_state                   as string)    as seller_state,
        cast(price                          as float64)   as price,
        cast(freight_value                  as float64)   as freight_value,
        cast(total_item_value               as float64)   as total_item_value,
        cast(payment_type                   as string)    as payment_type,
        cast(payment_value                  as float64)   as payment_value,
        cast(review_score                   as float64)   as review_score

    from source

)

select * from cleaned
