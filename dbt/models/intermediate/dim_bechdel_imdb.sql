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
-- there seemed to be 10 rows with NULL imdbid in
-- bechdel. Searching for this in title_basics, no 
-- matching results were found for these imdbids. 
-- This might be incorrectly inputted records. 
-- Either way, we just exclude these 10 NULL rows.
WHERE imdbid IS NOT NULL
{% if var('is_test')==True %}
LIMIT 10000
{% else %}
{% endif %}

