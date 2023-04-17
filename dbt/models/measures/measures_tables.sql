{{ config(materialized='table') }}

WITH bechdel_directors AS (
    SELECT *
    FROM {{ ref('dim_bechdel_directors') }}
)