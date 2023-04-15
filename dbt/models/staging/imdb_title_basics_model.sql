{{ config(materialized='view') }}

WITH title_basics AS (
    SELECT 
        tconst,
        titleType,
        primaryTitle,
        originalTitle,
        isAdult,
        CAST(
            EXTRACT(YEAR FROM startYear) AS INT64 
        ) AS startYear,
        SPLIT(genres, ',') AS genre
    FROM {{ source('staging', 'imdb_title_basics') }}
)

SELECT 
    tconst,
    titleType,
    primaryTitle,
    originalTitle,
    isAdult,
    startYear,
    genre
FROM title_basics
CROSS JOIN UNNEST(title_basics.genre) AS genre