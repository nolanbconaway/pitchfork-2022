{{ config(
        materialized='create'
        , post_hook='create unique index idx_review on {{ this.name }} (review_url)'
  ) 
}}
create table {{ this }} (
    review_url varchar not null primary key
    , is_multi_review boolean not null
    , pub_date datetime not null
    , body text not null
)
