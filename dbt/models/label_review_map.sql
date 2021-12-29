{{ config(
        materialized='create'
        , post_hook='create unique index idx_label_review on {{ this.name }} (review_url, label)'
  ) 
}}

create table {{ this }} (
    review_url varchar not null
    , label varchar not null
    , primary key (review_url, label)
    , foreign key (review_url) references {{ ref('reviews').name }}(review_url)
)
