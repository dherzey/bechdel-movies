{{ config(materialized='view') }}

WITH title_basics AS (
    SELECT *
    FROM {{ ref('imdb_title_basics_model') }}
),

name_basics AS (
    SELECT *
    FROM {{ ref('imdb_name_basics_model') }}
)

SELECT 
    t.tconst AS imdbid,
    t.primaryTitle,
    t.genre,
    n.nconst,
    n.primaryName,
    n.primaryProfession
FROM title_basics AS t
    INNER JOIN name_basics AS n
    ON t.tconst = n.tconst
{% if var('is_test')==True %}
LIMIT 10000
{% else %}
{% endif %}