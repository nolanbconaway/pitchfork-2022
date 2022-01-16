{{ config(materialized='view') }}
-- a flat model of all reviews, for common aggs.
-- this view does aggs and joins on indexes, so it should be fairly efficient.

with artist_group as (
    select
        artist_review_map.review_url
        , group_concat(artists.name, ', ') as artists
        , count(*) as artist_count
    from {{ ref('artists') }} as artists
    inner join {{ ref('artist_review_map') }} as artist_review_map
        on artists.artist_id = artist_review_map.artist_id
    group by artist_review_map.review_url
)

, author_group as (
    select
        review_url
        , group_concat(author, ', ') as authors
    from {{ ref('author_review_map') }}
    group by review_url
)

, genre_group as (
    select
        review_url
        , group_concat(genre, ', ') as genres
    from {{ ref('genre_review_map') }}
    group by review_url
)

, label_group as (
    select
        review_url
        , group_concat(label, ', ') as labels
    from {{ ref('label_review_map') }}
    group by review_url
)

, tombstone_group as (
    select
        review_url
        , group_concat(title, ', ') as title
        , group_concat(score, ', ') as score
        , group_concat(best_new_music, ', ') as best_new_music
        , group_concat(best_new_reissue, ', ') as best_new_reissue
        , count(*) as releases_reviewed
    from {{ ref('tombstones') }}
    group by review_url
)

, release_year_group as (
    select
        tombstones.review_url
        , group_concat(release_year_map.release_year, ', ') as release_year
        , count(*) > 1 as has_multiple_release_years
    from {{ ref('tombstones') }} as tombstones
    inner join {{ ref('tombstone_release_year_map') }} as release_year_map
        on tombstones.review_tombstone_id = release_year_map.review_tombstone_id
    group by tombstones.review_url
)

select
    reviews.review_url

    -- metas
    , reviews.is_multi_review
    , reviews.is_sunday_review
    , artist_group.artist_count as artist_count
    , tombstone_group.releases_reviewed
    , release_year_group.has_multiple_release_years

    -- core info
    , artist_group.artists as artists
    , tombstone_group.title
    , tombstone_group.score
    , tombstone_group.best_new_music
    , tombstone_group.best_new_reissue
    , author_group.authors as authors
    , genre_group.genres as genres
    , label_group.labels as labels

    -- dates
    , reviews.pub_date
    , release_year_group.release_year

    -- save this big payload for last.
    , reviews.body

from {{ ref('reviews') }} as reviews
inner join tombstone_group
    on tombstone_group.review_url = reviews.review_url
left join artist_group
    on reviews.review_url = artist_group.review_url
left join author_group
    on reviews.review_url = author_group.review_url
left join genre_group
    on reviews.review_url = genre_group.review_url
left join label_group
    on reviews.review_url = label_group.review_url
left join release_year_group
    on reviews.review_url = release_year_group.review_url
