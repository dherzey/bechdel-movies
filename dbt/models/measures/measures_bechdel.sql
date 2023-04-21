{{ config(materialized='table') }}

WITH bechdel AS (
    SELECT *
    FROM {{ ref('bechdel_transform_model') }}
)

SELECT
    EXTRACT(YEAR FROM 
        PARSE_DATE('%Y', CAST(year AS STRING))
    ) AS year,
    ratingRemark,
    COUNT(id) AS movieCount
FROM bechdel
GROUP BY 
    year, 
    ratingRemark
ORDER BY year