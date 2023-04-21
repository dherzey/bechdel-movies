{{ config(materialized='table') }}

WITH bechdel AS (
    SELECT *
    FROM {{ ref('bechdel_transform_model') }}
)

SELECT
    year,
    ratingRemark,
    COUNT(id) AS movieCount
FROM bechdel
GROUP BY 
    year, 
    ratingRemark
ORDER BY year