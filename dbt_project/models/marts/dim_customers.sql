{{
    config(
        materialized='table'
    )
}}

with customers as (
    select * from {{ ref('stg_customers') }}
),

orders as (
    select * from {{ ref('int_orders_enriched') }}
),

customer_metrics as (
    select
        customer_id,
        count(distinct order_id) as total_orders,
        sum(quantity) as total_items_purchased,
        sum(order_total) as lifetime_value,
        sum(order_profit) as lifetime_profit,
        avg(order_total) as avg_order_value,
        min(order_date) as first_order_date,
        max(order_date) as last_order_date,
        count(distinct product_category) as unique_categories_purchased,
        count(distinct case when status = 'completed' then order_id end) as completed_orders
    from orders
    group by 1
),

final as (
    select
        -- Primary key
        c.customer_id,
        
        -- Customer attributes
        c.first_name,
        c.last_name,
        c.full_name,
        c.email,
        c.country,
        c.customer_created_at,
        
        -- Metrics
        coalesce(m.total_orders, 0) as total_orders,
        coalesce(m.total_items_purchased, 0) as total_items_purchased,
        coalesce(m.lifetime_value, 0) as lifetime_value,
        coalesce(m.lifetime_profit, 0) as lifetime_profit,
        coalesce(m.avg_order_value, 0) as avg_order_value,
        coalesce(m.completed_orders, 0) as completed_orders,
        coalesce(m.unique_categories_purchased, 0) as unique_categories_purchased,
        m.first_order_date,
        m.last_order_date,
        
        -- Derived metrics
        case
            when m.total_orders > 0 then
                round(m.completed_orders::numeric / m.total_orders * 100, 2)
            else 0
        end as order_completion_rate,
        
        -- Customer segmentation
        case
            when coalesce(m.lifetime_value, 0) >= 500 then 'Platinum'
            when coalesce(m.lifetime_value, 0) >= 250 then 'Gold'
            when coalesce(m.lifetime_value, 0) >= 100 then 'Silver'
            else 'Bronze'
        end as customer_segment,
        
        -- Activity status
        case
            when m.last_order_date >= current_date - interval '90 days' then 'Active'
            when m.last_order_date >= current_date - interval '180 days' then 'At Risk'
            when m.total_orders > 0 then 'Churned'
            else 'Never Purchased'
        end as activity_status
        
    from customers c
    left join customer_metrics m on c.customer_id = m.customer_id
)

select * from final
