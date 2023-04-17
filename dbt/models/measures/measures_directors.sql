{{ config(materialized='table') }}

WITH bechdel_directors AS (
    SELECT *
    FROM {{ ref('dim_bechdel_directors') }}
)

SELECT
    directorName,
    bechdelRatingRemark,
    COUNT(imdbid) AS movieCount
FROM bechdel_directors
WHERE directorName IS NOT NULL
GROUP BY 
    directorName, 
    bechdelRatingRemark
ORDER BY 
    directorName,
    bechdelRatingRemark