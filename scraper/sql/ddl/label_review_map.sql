create table label_review_map (
    review_url varchar not null
    , label varchar not null
    , primary key (review_url, label)
    , foreign key (review_url) references reviews(review_url)
)
