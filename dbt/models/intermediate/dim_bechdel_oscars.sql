{{ config(materialized='view') }}

WITH bechdel_imdb AS (
    SELECT *
    FROM {{ ref('dim_bechdel_imdb') }}
),

oscars AS (
    SELECT 
        Movie,
        AwardCategory,
        AWardStatus,        
        CAST(AwardYear AS INT64) AS AwardYear,
        AwardCeremonyNum
    FROM {{ source('staging', 'oscars') }}
    WHERE AwardYear NOT LIKE '%/%'
),

oscars_doubledate AS (
    SELECT *
    FROM {{ source('staging', 'oscars') }}
    WHERE AwardYear LIKE '%/%'
)

SELECT DISTINCT
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
    AND b.startYear = o.AwardYear
{% if var('is_test_run', default=True) %}
LIMIT 1000
{% endif %}

UNION ALL

SELECT DISTINCT
    b.imdbid,
    b.primaryTitle,
    b.startYear,
    o.AwardCeremonyNum AS oscarsCeremony,
    o.AwardCategory AS oscarsCategory,
    o.AwardStatus AS oscarsStatus,
    b.bechdelRating,
    b.bechdelRatingRemark,
FROM bechdel_imdb AS b
    LEFT JOIN oscars_doubledate AS o
    ON b.primaryTitle = o.Movie
{% if var('is_test_run', default=True) %}
LIMIT 1000
{% endif %}