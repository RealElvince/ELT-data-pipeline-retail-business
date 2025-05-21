WITH invoice_numbers_cte AS(

    SELECT
        {{ dbt_utils.generate_surrogate_key(['Country']) }} AS country_id,
        Country AS country,
        {{ number_of_invoices('InvoiceNo') }} AS invoice_count
    FROM
        {{ ref('stg_online_retail') }}
    WHERE InvoiceNo IS NOT NULL
    GROUP BY Country
)

SELECT *
FROM invoice_numbers_cte
ORDER BY invoice_count DESC