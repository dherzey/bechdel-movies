{{ config(materialized='view') }}

WITH title_crew AS (
    SELECT 
        tconst,
        SPLIT(directors, ',') AS director,
        SPLIT(writers, ',') AS writer
    FROM {{ source('staging', 'imdb_title_crew') }}
)

SELECT 
    tconst,
    director,
    writer
FROM ( 
        title_crew
        CROSS JOIN UNNEST(title_crew.director) AS director
     )
    CROSS JOIN UNNEST(title_crew.writer) AS writer

{% if var('is_test_run', default=True) %}
LIMIT 1000
{% endif %}