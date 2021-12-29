{{ config(
        materialized='create'
        , post_hook='create unique index idx_genre_review on {{ this.name }} (review_url, genre)'
  ) 
}}

create table {{ this }} (
    review_url varchar not null
    , genre varchar not null
    , primary key (review_url, genre)
    , foreign key (review_url) references {{ ref('reviews').name }}(review_url)
)
