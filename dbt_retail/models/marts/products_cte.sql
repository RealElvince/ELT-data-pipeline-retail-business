WITH product_cte AS(
    SELECT DISTINCT 
        {{dbt_utils.generate_surrogate_key(['StockCode','Description'])}} AS product_id,
        StockCode AS stock_code,
        Description AS description
    FROM {{ ref('stg_online_retail')}}
    WHERE Description IS NOT NULL
    AND StockCode IS NOT NULL
)

SELECT *
FROM
    product_cte