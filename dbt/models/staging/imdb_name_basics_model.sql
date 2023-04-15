{{ config(materialized='view') }}

WITH name_basics AS (
    SELECT 
        nconst,
        primaryName,
        birthYear,
        primaryProfession,
        SPLIT(knownForTitles, ',') AS knownForTitle
    FROM {{ source('staging', 'imdb_name_basics') }}
)

SELECT 
    tconst,
    nconst,
    primaryName,
    birthYear,
    primaryProfession
FROM name_basics
CROSS JOIN UNNEST(name_basics.knownForTitle) AS tconst

{% if var('is_test_run', default=True) %}
LIMIT 1000
{% endif %}