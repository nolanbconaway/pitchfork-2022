{{ config(
        materialized='create'
        , post_hook='create unique index idx_review_tombstone on {{ this.name }} (review_tombstone_id)'
  ) 
}}

create table {{ this }} (
    review_tombstone_id varchar primary key
    , review_url varchar not null
    , picker_index int not null
    , title varchar not null
    , score real not null
    , best_new_music boolean  -- null if pub date before jan 15 2003
    , best_new_reissue boolean  -- null if pub date before jan 8 2009
    , foreign key (review_url) references {{ ref('reviews').name }}(review_url)
)
