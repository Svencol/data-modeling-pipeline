{{
    config(
        materialized='view'
    )
}}

with source as (
    select * from {{ source('raw', 'customers') }}
),

deduplicated as (
    -- Take most recent record per customer_id
    select
        *,
        row_number() over (
            partition by customer_id 
            order by _loaded_at desc
        ) as row_num
    from source
),

cleaned as (
    select
        -- Primary key
        customer_id,
        
        -- Attributes
        trim(first_name) as first_name,
        trim(last_name) as last_name,
        trim(first_name) || ' ' || trim(last_name) as full_name,
        lower(trim(email)) as email,
        trim(country) as country,
        
        -- Timestamps
        created_at as customer_created_at,
        
        -- Metadata
        _loaded_at,
        _source
        
    from deduplicated
    where row_num = 1
)

select * from cleaned
