{{
    config(
        materialized='view'
    )
}}

with orders as (
    select * from {{ ref('stg_orders') }}
),

customers as (
    select * from {{ ref('stg_customers') }}
),

products as (
    select * from {{ ref('stg_products') }}
),

enriched as (
    select
        -- Order keys
        o.order_id,
        o.customer_id,
        o.product_id,
        
        -- Customer info
        c.full_name as customer_name,
        c.email as customer_email,
        c.country as customer_country,
        
        -- Product info
        p.product_name,
        p.category as product_category,
        p.price as unit_price,
        p.cost as unit_cost,
        p.profit_margin_pct,
        
        -- Order details
        o.quantity,
        o.status,
        
        -- Calculated amounts
        round(p.price * o.quantity, 2) as order_total,
        round(p.cost * o.quantity, 2) as order_cost,
        round((p.price - p.cost) * o.quantity, 2) as order_profit,
        
        -- Time dimensions
        o.order_date,
        o.order_date_day,
        o.order_year,
        o.order_month,
        o.order_day_of_week,
        
        -- Metadata
        o._loaded_at
        
    from orders o
    left join customers c on o.customer_id = c.customer_id
    left join products p on o.product_id = p.product_id
)

select * from enriched
