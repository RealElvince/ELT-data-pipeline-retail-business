WITH customer_cte AS (
    SELECT DISTINCT
        {{dbt_utils.generate_surrogate_key(['CustomerID','Country'])}} AS customer_id,
        CustomerID AS original_customer_id,
        Country as country
    FROM {{ ref('stg_online_retail')}}
    WHERE Country IS NOT NULL
)

SELECT *
FROM
    customer_cte
