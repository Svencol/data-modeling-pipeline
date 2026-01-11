{{
    config(
        materialized='view'
    )
}}

with source as (
    select * from {{ source('raw', 'products') }}
),

deduplicated as (
    select
        *,
        row_number() over (
            partition by product_id 
            order by _loaded_at desc
        ) as row_num
    from source
),

cleaned as (
    select
        -- Primary key
        product_id,
        
        -- Attributes
        trim(product_name) as product_name,
        trim(category) as category,
        
        -- Numeric fields
        round(price::numeric, 2) as price,
        round(cost::numeric, 2) as cost,
        
        -- Calculated fields
        round(((price - cost) / nullif(price, 0)) * 100, 2) as profit_margin_pct,
        round(price - cost, 2) as profit_per_unit,
        
        -- Metadata
        _loaded_at,
        _source
        
    from deduplicated
    where row_num = 1
)

select * from cleaned
