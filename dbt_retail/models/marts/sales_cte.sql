WITH sales_cte AS(
    SELECT
        InvoiceNo AS invoice_number,
        StockCode AS product_code,
        Quantity AS quantity,
        UnitPrice AS unit_price,
        {{ dbt_utils.generate_surrogate_key(['InvoiceNo', 'StockCode']) }} AS sales_id,
        {{ calculate_total_revenue('Quantity', 'UnitPrice') }} AS total_revenue
    FROM {{ ref('stg_online_retail') }}
    WHERE Quantity > 0 AND UnitPrice > 0
)

SELECT *
FROM sales_cte