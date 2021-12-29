{{ config(
        materialized='create'
        , post_hook='create unique index idx_author_review on {{ this.name }} (review_url, author)'
  ) 
}}

create table {{ this }} (
    review_url varchar not null
    , author varchar not null
    , primary key (review_url, author)
    , foreign key (review_url) references {{ ref('reviews').name }}(review_url)
)
