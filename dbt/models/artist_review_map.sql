{{ config(
        materialized='create'
        , post_hook='create index idx_artist_review on {{ this.name }} (review_url, artist_id)'
  ) 
}}

create table {{ this }} (
    review_url varchar not null
    , artist_id varchar not null
    , primary key (review_url, artist_id)
    , foreign key (review_url) references {{ ref('reviews').name }}(review_url)
    , foreign key (artist_id) references {{ ref('artists').name }}(artist_id)
)
