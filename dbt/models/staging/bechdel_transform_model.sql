{{ config(materialized='view') }}

SELECT *,
    CASE
        WHEN rating = 1 THEN "failed"
        WHEN rating = 2 THEN "failed"
        ELSE "passed"
    END AS ratingRemark
FROM {{ source('staging', 'bechdel') }}