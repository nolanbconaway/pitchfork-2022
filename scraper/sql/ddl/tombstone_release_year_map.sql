create table tombstone_release_year_map (
    review_tombstone_id varchar
    , release_year int not null
    , primary key (review_tombstone_id, release_year)
    , foreign key (
        review_tombstone_id
    ) references tombstones(review_tombstone_id)
)
