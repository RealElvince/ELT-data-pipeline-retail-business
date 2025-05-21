SELECT *
FROM {{ ref('invoice_number_per_country') }}
WHERE invoice_count < 0
