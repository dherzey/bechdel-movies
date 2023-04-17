{{ config(materialized='view') }}

WITH bechdel AS (
    SELECT *
    FROM {{ ref('bechdel_transform_model') }}
),

title_basics AS (
    SELECT *
    FROM {{ ref('imdb_title_basics_model') }}
)

SELECT
    t.tconst AS imdbid,
    t.primaryTitle,
    t.startYear,
    t.isAdult,
    t.genre,
    b.rating AS bechdelRating,
    b.ratingRemark AS bechdelRatingRemark
FROM bechdel AS b
    LEFT JOIN title_basics AS t
    ON b.imdbid = t.tconst 
    AND t.startYear = b.year