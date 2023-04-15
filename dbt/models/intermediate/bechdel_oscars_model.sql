{{ config(materialized='view') }}

WITH bechdel AS (
    SELECT *
    FROM {{ ref('bechdel_transform_model') }}
),

title_basics AS (
    SELECT *
    FROM {{ ref('imdb_title_basics_model') }}
),

oscars AS (
    SELECT *
    FROM {{ source('intermediate', 'oscars') }}
),

bechdel_title AS (
    SELECT
        t.tconst AS imdbid,
        t.primaryTitle,
        t.originalTitle,
        t.startYear,
        t.isAdult,
        t.genre,
        b.rating AS bechdelRating,
        b.ratingRemark AS bechdelRatingRemark
    FROM bechdel AS b
        LEFT JOIN title_basics AS t
        ON b.imdbid = t.tconst 
        AND t.startYear = b.year
)

SELECT
    b.imdbid,
    b.primaryTitle,
    b.originalTitle,
    b.startYear,
    b.isAdult,
    b.genre,
    b.bechdelRating,
    b.bechdelRatingRemark,
    o.AwardCeremonyNum AS oscarsCeremony,
    o.AwardCategory AS oscarsCategory,
    o.AwardStatus AS oscarsStatus
FROM bechdel_title AS b
LEFT JOIN oscars AS o
ON b.primaryTitle = o.Movie