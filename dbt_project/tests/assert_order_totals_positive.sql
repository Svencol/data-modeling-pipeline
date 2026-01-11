-- Test that all order totals are positive
-- This ensures data integrity for financial calculations

select
    order_id,
    order_total
from {{ ref('fct_orders') }}
where order_total <= 0
