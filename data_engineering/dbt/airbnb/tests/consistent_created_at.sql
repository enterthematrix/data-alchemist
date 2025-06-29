SELECT
    *
FROM
    {{ ref('fact_reviews') }} as fact_reviews
    join {{ ref('dim_listings_cleansed') }} as dim_listings_cleansed
    USING  (listing_id)
WHERE dim_listings_cleansed.created_at >= fact_reviews.review_date