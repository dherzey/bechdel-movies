{{ config(materialized='view') }}

WITH bechdel_imdb AS (
    SELECT *
    FROM {{ ref('dim_bechdel_imdb') }}
),

title_ratings AS (
    SELECT *
    FROM {{ source('staging', 'imdb_title_ratings') }}
)

SELECT DISTINCT
    b.imdbid,
    b.primaryTitle,
    r.averageRating AS IMDBRating
    b.bechdelRating,
    b.bechdelRatingRemark
FROM bechdel_imdb AS b
    INNER JOIN title_ratings AS r
    ON b.imdbid = r.tconst
{% if var('is_test_run', default=True) %}
LIMIT 1000
{% endif %}