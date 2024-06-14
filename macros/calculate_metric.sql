{% macro calculate_metric(numerator, denominator, multiplier=100) %}
    coalesce({{ dbt_utils.safe_divide(numerator, denominator) }} * {{ multiplier }}, 0)
{% endmacro %}