create table genre_review_map (
    review_url varchar not null
    , genre varchar not null
    , primary key (review_url, genre)
    , foreign key (review_url) references reviews(review_url)
)
