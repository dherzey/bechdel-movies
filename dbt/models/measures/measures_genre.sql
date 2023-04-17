{{ config(materialized='table') }}

WITH bechdel AS (
    SELECT *
    FROM {{ ref('dim_bechdel_imdb') }}
)

SELECT
    genre,
    bechdelRatingRemark,
    COUNT(imdbid) AS movieCount
FROM bechdel
WHERE genre IS NOT NULL
GROUP BY 
    genre, 
    bechdelRatingRemark
ORDER BY genre