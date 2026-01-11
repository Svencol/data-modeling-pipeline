-- Test that lifetime value is non-negative for all customers
-- Validates that our aggregation logic is correct

select
    customer_id,
    lifetime_value
from {{ ref('dim_customers') }}
where lifetime_value < 0
