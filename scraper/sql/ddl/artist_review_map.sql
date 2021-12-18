create table artist_review_map (
    review_url varchar not null
    , artist_id varchar not null
    , primary key (review_url, artist_id)
    , foreign key (review_url) references reviews(review_url)
    , foreign key (artist_id) references artists(artist_id)
)
