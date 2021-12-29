{{ config(
        materialized='create'
        , post_hook='create unique index idx_artist on {{ this.name }} (artist_id)'
  ) 
}}
create table {{ this }} (
    artist_id varchar not null primary key
    , "name" varchar not null
    , artist_url varchar
)
