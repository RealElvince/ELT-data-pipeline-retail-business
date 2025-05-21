WITH quantity_cte AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['Country']) }} AS country_id,
        Country AS country,
        {{ calculate_total_quantity('Quantity') }} AS total_quantity
    FROM
        {{ ref('stg_online_retail') }}
    WHERE Quantity > 0
    GROUP BY Country
)

SELECT *
FROM quantity_cte
ORDER BY total_quantity DESC
