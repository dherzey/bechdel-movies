{{ config(materialized='table') }}

WITH bechdel AS (
    SELECT *
    FROM {{ ref('dim_bechdel_imdb') }}
),

total AS (
    SELECT
        genre,
        COUNT(imdbid) AS totalMovieCount
    FROM bechdel
    WHERE genre IS NOT NULL
    GROUP BY 
        genre
    ORDER BY movieCount DESC
),

genre_score AS (
    SELECT
        genre,
        bechdelRatingRemark,
        COUNT(imdbid) AS movieCount
    FROM bechdel
    WHERE genre IS NOT NULL
    GROUP BY 
        genre, 
        bechdelRatingRemark
    ORDER BY genre
)

SELECT 
    gs.genre,
    gs.bechdelRatingRemark,
    gs.movieCount,
    t.totalMovieCount
FROM genre_score AS gs
    INNER JOIN total AS t
    ON gs.genre = t.genre
ORDER BY
    genre,
    movieCount