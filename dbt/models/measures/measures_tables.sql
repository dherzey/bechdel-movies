{{ config(materialized='table') }}

WITH bechdel AS (
    SELECT 
        'bechdel' AS tableName,
        COUNT(*) AS rowCount,
        COUNT(DISTINCT CONCAT(title, year)) AS movieCount,
        CAST(MIN(year) AS STRING) AS oldestYear,
        CAST(MAX(year) AS STRING) AS latestYear
    FROM {{ ref('bechdel_transform_model') }}
),

oscars AS (
    SELECT
        'oscars' AS tableName,
        COUNT(*) AS rowCount,
        COUNT(DISTINCT CONCAT(Movie, AwardYear)) AS movieCount,
        MIN(AwardYear) AS oldestYear,
        MAX(AwardYear) AS latestYear
    FROM {{ source('staging','oscars') }}
),

imdb_title AS (
    SELECT
        'imdb_title_basics' AS tableName,
        COUNT(*) AS rowCount,
        COUNT(DISTINCT CONCAT(primaryTitle, startYear)) AS movieCount,
        CAST(MIN(startYear) AS STRING) AS oldestYear,
        CAST(MAX(startYear) AS STRING) AS latestYear
    FROM {{ ref('imdb_title_basics_model') }}
)

SELECT * FROM bechdel
UNION ALL
SELECT * FROM oscars
UNION ALL
SELECT * FROM imdb_title
{% if var('is_test', default=True) %}
LIMIT 1000
{% endif %}