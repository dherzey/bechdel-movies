{{ config(materialized='view') }}

WITH bechdel_imdb AS (
    SELECT *
    FROM {{ ref('bechdel_imdb_model') }}
),

title_ratings AS (
    SELECT *
    FROM {{ source('staging', 'imdb_title_ratings') }}
)

SELECT 
    b.imdbid,
    b.primaryTitle,
    b.bechdelRating,
    b.bechdelRatingRemark,
    r.averageRating AS IMDBRating
FROM bechdel_imdb AS b
INNER JOIN title_ratings AS r
ON b.imdbid = r.tconst