
WITH country_cte AS (
    SELECT DISTINCT
        Country,
        UPPER(Country) AS country_upper
    FROM {{ ref('stg_online_retail') }}
    WHERE Country IS NOT NULL
)

SELECT *
FROM country_cte
