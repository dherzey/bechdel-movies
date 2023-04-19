{{ config(materialized='view') }}

WITH bechdel_imdb AS (
    SELECT *
    FROM {{ ref('dim_bechdel_imdb') }}
),

title_crew AS (
    SELECT *
    FROM {{ ref('imdb_title_crew_model') }}
),

name_basics AS (
    SELECT *
    FROM {{ ref('imdb_name_basics_model') }}
),

bechdel_director AS (
    SELECT
        b.imdbid,
        b.primaryTitle,
        b.genre,
        c.director,
        b.bechdelRating,
        b.bechdelRatingRemark
    FROM bechdel_imdb AS b
        LEFT JOIN title_crew AS c
        ON b.imdbid = c.tconst
)

SELECT DISTINCT
    b.imdbid,
    b.primaryTitle,
    b.genre,
    n.primaryName AS directorName,
    b.bechdelRating,
    b.bechdelRatingRemark
FROM bechdel_director AS b
    LEFT JOIN name_basics AS n
    ON b.director = n.nconst
{% if var('is_test', default=True) %}
LIMIT 1000
{% endif %}