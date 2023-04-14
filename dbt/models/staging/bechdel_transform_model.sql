{{ config(materialized='view') }}

SELECT *,
    CASE
        WHEN rating = 3 THEN 'passed'
        ELSE "failed"
    END AS ratingRemark
FROM {{ source('staging', 'bechdel') }}
WHERE imdbid IS NOT NULL