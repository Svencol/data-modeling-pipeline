{{
    config(
        materialized='table'
    )
}}

with products as (
    select * from {{ ref('stg_products') }}
),

orders as (
    select * from {{ ref('int_orders_enriched') }}
),

product_metrics as (
    select
        product_id,
        count(distinct order_id) as total_orders,
        count(distinct customer_id) as unique_customers,
        sum(quantity) as total_quantity_sold,
        sum(order_total) as total_revenue,
        sum(order_profit) as total_profit,
        avg(order_total) as avg_order_value,
        min(order_date) as first_sale_date,
        max(order_date) as last_sale_date
    from orders
    group by 1
),

final as (
    select
        -- Primary key
        p.product_id,
        
        -- Product attributes
        p.product_name,
        p.category,
        p.price,
        p.cost,
        p.profit_margin_pct,
        p.profit_per_unit,
        
        -- Sales metrics
        coalesce(m.total_orders, 0) as total_orders,
        coalesce(m.unique_customers, 0) as unique_customers,
        coalesce(m.total_quantity_sold, 0) as total_quantity_sold,
        coalesce(m.total_revenue, 0) as total_revenue,
        coalesce(m.total_profit, 0) as total_profit,
        coalesce(m.avg_order_value, 0) as avg_order_value,
        m.first_sale_date,
        m.last_sale_date,
        
        -- Derived metrics
        case
            when coalesce(m.total_quantity_sold, 0) >= 10 then 'Best Seller'
            when coalesce(m.total_quantity_sold, 0) >= 5 then 'Good Performer'
            when coalesce(m.total_quantity_sold, 0) >= 1 then 'Moderate'
            else 'No Sales'
        end as sales_tier,
        
        -- Price segment
        case
            when p.price >= 200 then 'Premium'
            when p.price >= 100 then 'Mid-Range'
            when p.price >= 50 then 'Economy'
            else 'Budget'
        end as price_segment
        
    from products p
    left join product_metrics m on p.product_id = m.product_id
)

select * from final
