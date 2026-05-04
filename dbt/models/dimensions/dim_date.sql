-- dim_date.sql
-- Calendar dimension derived from order purchase dates.

{{ config(materialized='table') }}

with date_spine as (

    select distinct order_purchase_date as date_key
    from {{ ref('stg_orders') }}
    where order_purchase_date is not null

)

select
    date_key,
    extract(year      from date_key) as year,
    extract(month     from date_key) as month,
    extract(day       from date_key) as day,
    extract(dayofweek from date_key) as day_of_week,
    format_date('%A',    date_key)   as day_name,
    format_date('%B',    date_key)   as month_name,
    format_date('%Y-%m', date_key)   as year_month,
    case
        when extract(dayofweek from date_key) in (1, 7) then true
        else false
    end                              as is_weekend

from date_spine
order by date_key
