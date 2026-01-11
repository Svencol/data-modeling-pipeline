{{
    config(
        materialized='view'
    )
}}

with source as (
    select * from {{ source('raw', 'orders') }}
),

deduplicated as (
    select
        *,
        row_number() over (
            partition by order_id 
            order by _loaded_at desc
        ) as row_num
    from source
),

cleaned as (
    select
        -- Primary key
        order_id,
        
        -- Foreign keys
        customer_id,
        product_id,
        
        -- Order details
        quantity,
        lower(trim(status)) as status,
        
        -- Timestamps
        order_date,
        date(order_date) as order_date_day,
        extract(year from order_date) as order_year,
        extract(month from order_date) as order_month,
        extract(dow from order_date) as order_day_of_week,
        
        -- Metadata
        _loaded_at,
        _source
        
    from deduplicated
    where row_num = 1
)

select * from cleaned
