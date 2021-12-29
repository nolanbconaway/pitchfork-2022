{{ config(
        materialized='create'
        , post_hook='create unique index idx_tombstone_year on {{ this.name }} (review_tombstone_id, release_year)'
  ) 
}}

create table {{ this }} (
    review_tombstone_id varchar
    , release_year int not null
    , primary key (review_tombstone_id, release_year)
    , foreign key (
        review_tombstone_id
    ) references {{ ref('tombstones').name }}(review_tombstone_id)
)
