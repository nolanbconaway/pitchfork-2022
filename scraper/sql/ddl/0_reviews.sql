create table reviews (
    review_url varchar not null primary key
    , is_multi_review boolean
    , pub_date datetime not null
    , body text not null
)
