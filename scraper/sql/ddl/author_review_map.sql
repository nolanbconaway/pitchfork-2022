create table author_review_map (
    review_url varchar not null
    , author varchar not null
    , primary key (review_url, author)
    , foreign key (review_url) references reviews(review_url)
)
