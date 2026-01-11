{{
    config(
        materialized='table'
    )
}}

with enriched_orders as (
    select * from {{ ref('int_orders_enriched') }}
),

final as (
    select
        -- Keys
        order_id,
        customer_id,
        product_id,
        
        -- Customer dimensions
        customer_name,
        customer_country,
        
        -- Product dimensions
        product_name,
        product_category,
        
        -- Order facts
        quantity,
        unit_price,
        unit_cost,
        order_total,
        order_cost,
        order_profit,
        profit_margin_pct,
        status,
        
        -- Time dimensions
        order_date,
        order_date_day,
        order_year,
        order_month,
        order_day_of_week,
        
        -- Derived flags
        case 
            when status = 'completed' then true 
            else false 
        end as is_completed,
        
        case
            when order_total >= 300 then 'High'
            when order_total >= 100 then 'Medium'
            else 'Low'
        end as order_value_tier
        
    from enriched_orders
)

select * from final
