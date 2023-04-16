{{ config(materialized='table') }}

WITH bechdel_oscars AS (
    SELECT *
    FROM {{ ref('bechdel_oscars_model') }}
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