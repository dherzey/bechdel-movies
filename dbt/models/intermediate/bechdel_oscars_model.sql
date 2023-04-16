{{ config(materialized='view') }}

WITH bechdel_imdb AS (
    SELECT *
    FROM {{ ref('bechdel_imdb_model') }}
),

oscars AS (
    SELECT *
    FROM {{ source('staging', 'oscars') }}
)

SELECT
    b.imdbid,
    b.primaryTitle,
    b.startYear,
    o.AwardCeremonyNum AS oscarsCeremony,
    o.AwardCategory AS oscarsCategory,
    o.AwardStatus AS oscarsStatus,
    b.bechdelRating,
    b.bechdelRatingRemark,
FROM bechdel_imdb AS b
    LEFT JOIN oscars AS o
    ON b.primaryTitle = o.Movie