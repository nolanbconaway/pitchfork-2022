{{ config(materialized='view') }}
-- a flat model of all standard reviews; exclude reissues, multi reviews, etc.
-- i.e., the usual p4k review.

select
    review_url

    -- metas
    , artist_count
    , artists
    , title
    , cast(score as real) as score
    , cast(best_new_music as boolean) as best_new_music
    , authors
    , genres
    , labels
    , pub_date
    , cast(release_year as int) as release_year
    , body

from {{ ref('reviews_flat') }}

/*
RULE: a standard review was written in the year of, or immediately prior/after the
release year. It does not have multiple releases reviewed and cannot have multiple
release years.

Justification: sometimes p4k reviews UPCOMING releases in like december. Sometimes
p4k reviews a release from the prior month in january. Mostly p4k reviews a release in
the year of.
*/
where is_standard_review
