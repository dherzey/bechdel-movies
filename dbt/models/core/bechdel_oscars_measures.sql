{{ config(materialized='table') }}

WITH bechdel_oscars AS (
    SELECT *
    FROM {{ ref('bechdel_oscars_model') }}
)

SELECT
    oscarsCategory,
    oscarsStatus,
    bechdelRatingRemark,
    COUNT(imdbid) AS movieCount
FROM bechdel_oscars
WHERE 
    oscarsCeremony IS NOT NULL
    AND oscarsStatus = 'won'
GROUP BY 
    oscarsCategory,
    oscarsStatus, 
    bechdelRatingRemark
ORDER BY 
    oscarsCategory,
    oscarsStatus,
    bechdelRatingRemark