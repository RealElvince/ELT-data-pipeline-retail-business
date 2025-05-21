{% macro calculate_total_quantity(quantity_column)%}
 SUM({{quantity_column}})
{% endmacro %}