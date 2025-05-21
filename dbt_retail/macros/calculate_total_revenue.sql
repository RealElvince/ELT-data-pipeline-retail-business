{% macro calculate_total_revenue(quantity_column,price_column)%}
  ({{quantity_column}}*{{price_column}})
{% endmacro %}