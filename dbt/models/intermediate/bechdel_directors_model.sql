{{ config(materialized='view') }}

WITH bechdel_imdb AS (
    SELECT *
    FROM {{ ref('bechdel_imdb_model') }}
),

title_crew AS (
    SELECT *
    FROM {{ ref('imdb_title_crew_model') }}
),

title_name_crew AS (
    SELECT *
    FROM {{ ref('imdb_title_people_model') }}
),

bechdel_director AS (
    SELECT 
        b.imdbid,
        b.primaryTitle,
        b.originalTitle,
        c.director,
        b.bechdelRating,
        b.bechdelRatingRemark
    FROM bechdel_imdb AS b
    INNER JOIN title_crew AS c
    ON b.imdbid = c.tconst
)

SELECT 
    b.imdbid,
    b.primaryTitle,
    n.primaryName AS directorName,
    b.bechdelRating,
    b.bechdelRatingRemark
FROM bechdel_director AS b
INNER JOIN title_name_crew AS n
ON b.director = n.nconst
AND b.imdbid = n.tconst