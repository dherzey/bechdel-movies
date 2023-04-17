{{ config(materialized='view') }}

WITH name_basics AS (
    SELECT 
        nconst,
        primaryName,
        birthYear,
        SPLIT(primaryProfession, ',') AS primaryProfession,
        SPLIT(knownForTitles, ',') AS knownForTitle
    FROM {{ source('staging', 'imdb_name_basics') }}
)

SELECT 
    CAST(
        REGEXP_REPLACE(knownForTitle, '[^0-9]', '')
        AS INT64
    ) AS tconst,
    nconst,
    primaryName,
    birthYear,
    primaryProfession
FROM name_basics
CROSS JOIN UNNEST(name_basics.knownForTitle) AS knownForTitle
CROSS JOIN UNNEST(name_basics.primaryProfession) AS primaryProfession