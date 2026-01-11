{% macro calculate_margin(revenue, cost) %}
    case 
        when {{ revenue }} = 0 then 0
        else round((({{ revenue }} - {{ cost }}) / {{ revenue }}) * 100, 2)
    end
{% endmacro %}

{% macro date_spine(start_date, end_date) %}
    -- Generate a series of dates between start and end
    select
        generate_series(
            '{{ start_date }}'::date,
            '{{ end_date }}'::date,
            '1 day'::interval
        )::date as date_day
{% endmacro %}

{% macro safe_divide(numerator, denominator, default_value=0) %}
    case
        when {{ denominator }} = 0 or {{ denominator }} is null 
        then {{ default_value }}
        else {{ numerator }} / {{ denominator }}
    end
{% endmacro %}
