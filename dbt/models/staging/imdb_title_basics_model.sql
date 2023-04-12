{{ config(materialized='view') }}

WITH title_basics AS (
    SELECT 
        tconst,
        titleType,
        primaryTitle,
        originalTitle,
        isAdult,
        startYear,
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

{% if var('is_test_run', default=True) %}
LIMIT 1000
{% endif %}