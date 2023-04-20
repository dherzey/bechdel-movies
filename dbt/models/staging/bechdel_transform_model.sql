{{ config(materialized='view') }}

WITH bechdel_new AS (
    SELECT 
        title, 
        CAST(imdbid AS INT64) AS imdbid,
        id,
        year,
        rating,
        CASE
            WHEN rating = 3 THEN 'passed'
            ELSE "failed"
        END AS ratingRemark
    FROM {{ source('staging', 'bechdel') }}
),

non_unique AS (
    -- there are 18 non-unique values for the imdbid 
    -- and upon checking, there seemed to be duplicates
    -- or errors in associating imdbid correctly
    SELECT imdbid, COUNT(*)
    FROM bechdel_new
    GROUP BY imdbid
    HAVING COUNT(*) > 1
),

unique_full AS (
    SELECT *
    FROM bechdel_new
    WHERE imdbid NOT IN (
        SELECT imdbid FROM non_unique)
    AND imdbid IS NOT NULL
),

rank_title AS (
    -- get the first record from a duplicate
    -- in case of different ratings, get the
    -- highest rating by default
    SELECT *,
        RANK() OVER(PARTITION BY imdbid ORDER BY rating DESC) 
        AS rankFinal
    FROM (
        SELECT *,
            RANK() OVER(PARTITION BY imdbid, rating ORDER BY id) 
            AS rankTitle
        FROM bechdel_new
        WHERE imdbid IN (SELECT imdbid FROM non_unique)
    )
    WHERE rankTitle = 1
),

non_unique_final AS (
    SELECT
        title, 
        imdbid,
        id,
        year,
        rating,
        ratingRemark
    FROM rank_title
    WHERE rankFinal = 1
)

SELECT * FROM unique_full
UNION ALL
SELECT * FROM non_unique_final