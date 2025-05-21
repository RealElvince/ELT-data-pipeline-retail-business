{% macro number_of_invoices(invoice_no_column) %}
    COUNT(DISTINCT {{ invoice_no_column }})
{% endmacro %}