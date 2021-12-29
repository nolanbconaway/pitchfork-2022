create table tombstones (
    review_tombstone_id varchar primary key
    , review_url varchar not null
    , picker_index int not null
    , title varchar not null
    , score real not null
    , bnm boolean  -- null if pub date before jan 15 2003
    , foreign key ( review_url) references reviews(review_url)
)
