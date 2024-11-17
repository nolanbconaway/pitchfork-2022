{{ config(
        materialized='create'
        , post_hook='create unique index idx_tombstone_label on {{ this.name }} (review_tombstone_id, label)'
  ) 
}}

create table {{ this }} (
    review_tombstone_id varchar
    , label varchar not null
    , primary key (review_tombstone_id, label)
    , foreign key (
        review_tombstone_id
    ) references {{ ref('tombstones').name }}(review_tombstone_id)
)
