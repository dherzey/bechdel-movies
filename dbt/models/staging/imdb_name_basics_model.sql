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
    nconst,
    primaryName,
    birthYear,
    primaryProfession,
    knownForTitle
FROM name_basics
CROSS JOIN UNNEST(name_basics.knownForTitle) AS knownForTitle

{% if var('is_test_run', default=True) %}
LIMIT 1000
{% endif %}