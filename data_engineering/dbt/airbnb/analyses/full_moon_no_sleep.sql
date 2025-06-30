
-- analysis: contains ad-hoc queries for quick analysis for which there is no need to create a model
-- This analysis checks the sentiment of Airbnb reviews during full moon nights
WITH fullmoon_reviews AS (
    SELECT * FROM {{ ref('full_moon_reviews') }}
)
SELECT
    is_full_moon,
    review_sentiment,
    COUNT(*) as reviews
FROM
    fullmoon_reviews
GROUP BY
    is_full_moon,
    review_sentiment
ORDER BY
    is_full_moon,
    review_sentiment