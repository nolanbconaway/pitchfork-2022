"""Use the saved review data to build an analytics-ready SQLite db."""
import argparse
import gzip
import json
import multiprocessing
import random
import sqlite3
from pathlib import Path
from typing import Any, Generator, Iterable

from .models import Review
from tqdm import tqdm

from ._utils import dbt, FIRST_BEST_NEW_MUSIC, REVIEWS_SAVE_PATH, SQLITE_SAVE_PATH


def chunker(seq: Iterable, size: int) -> Generator:
    """Thx stackoverfow."""
    return (seq[pos : pos + size] for pos in range(0, len(seq), size))


def parse_args() -> argparse.Namespace:
    """Make the parser for the command line arguments."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--no-dbt",
        action="store_true",
        help="Option to Skip DBT steps (assuming theyre run separately).",
    )
    parser.add_argument(
        "--in",
        dest="in_",
        type=Path,
        help="The path to the saved reviews data.",
        default=REVIEWS_SAVE_PATH,
    )
    parser.add_argument(
        "--out",
        type=Path,
        help="The path to save the sqlite database.",
        default=SQLITE_SAVE_PATH,
    )
    parser.add_argument(
        "--single", type=Path, help="Run only a single file. Useful for debugging."
    )
    parser.add_argument(
        "--procs",
        type=lambda x: multiprocessing.cpu_count() if x == "max" else int(x),
        default=1,
        help="Run a random sample of files. Also useflul for debugging.",
    )
    args = parser.parse_args()
    return args


def insert_many(
    db: sqlite3.Connection,
    table_name: str,
    cols: list[str],
    vals: list[tuple[Any]],
    integrity_handler: str = "raise",
):
    """Wrapper around executemany for arbitrary tables."""
    assert integrity_handler in ("ignore", "raise", "warn")
    col_sql = ", ".join(map(lambda x: f'"{x}"', cols))
    qs_sql = ", ".join(["?"] * len(cols))

    assert all(len(i) == len(cols) for i in vals)

    sql = f"""
        insert into "{table_name}" ({col_sql}) 
        values ({qs_sql})
    """
    if integrity_handler == "ignore":
        sql += "\n on conflict do nothing"

    try:
        db.executemany(sql, vals)
    except sqlite3.IntegrityError:
        if integrity_handler == "warn":
            print(f"Warning: Integrity error ignored on {table_name}.")
        else:
            raise


def insert_review(db: sqlite3.Connection, review_url: str, review: Review):
    """Insert data into the db for a review."""
    insert_many(
        db,
        "reviews",
        ["review_url", "is_multi_review", "body", "pub_date"],
        [(review_url, review.is_multi_review, review.body, review.pub_date)],
    )

    insert_many(
        db,
        "artists",
        ["artist_id", "name", "artist_url"],
        [(artist.artist_id, artist.name, artist.url) for artist in review.artists],
        integrity_handler="ignore",  # ignore errors, since we only need new artists
    )

    insert_many(
        db,
        "tombstones",
        ["review_tombstone_id", "review_url", "picker_index", "title", "score", "bnm"],
        [
            (
                f"""{review_url}-{idx}""",
                review_url,
                idx,
                tombstone.title,
                tombstone.score,
                tombstone.bnm if review.pub_date >= FIRST_BEST_NEW_MUSIC else None,
            )
            for idx, tombstone in enumerate(review.tombstones)
        ],
    )

    insert_many(
        db,
        "artist_review_map",
        ["review_url", "artist_id"],
        [(review_url, artist.artist_id) for artist in review.artists],
    )

    insert_many(
        db,
        "genre_review_map",
        ["review_url", "genre"],
        [(review_url, genre) for genre in review.genres],
    )

    insert_many(
        db,
        "label_review_map",
        ["review_url", "label"],
        [(review_url, label) for label in review.labels],
    )
    insert_many(
        db,
        "author_review_map",
        ["review_url", "author"],
        [(review_url, author) for author in review.authors],
    )

    insert_many(
        db,
        "tombstone_release_year_map",
        ["review_tombstone_id", "release_year"],
        [
            (f"""{review_url}-{idx}""", year)
            for idx, tombstone in enumerate(review.tombstones)
            for year in (tombstone.release_years or [])
        ],
    )


if __name__ == "__main__":
    args = parse_args()
    if args.single is not None:
        assert args.single.exists()
    else:
        assert args.in_.exists()

    if args.out.exists() and not args.no_dbt:
        args.out.unlink()

    review_jsons = (
        tuple(args.in_.glob("*.json.gz")) if not args.single else [args.single]
    )

    if not args.no_dbt:
        print("Executing DBT clean...")
        dbt("clean")
        print()

        # build tables
        print("Executing DBT run...")
        dbt("run")
        print()

    # shared across later lines. idc about closing it, this is sqlite.
    db = sqlite3.connect(args.out, timeout=10000, check_same_thread=False)

    def f(fpath: Path) -> tuple[str, Review]:
        with gzip.open(fpath, "rb") as f:
            json_data = json.load(f)
        try:
            review = Review.from_html(json_data["html"])
        except Exception:
            print(f"Error parsing {fpath}")
            raise
        return json_data["url"], review

    chunks = list(chunker(review_jsons, min(1000, len(review_jsons))))

    print(f"Inserting data in {len(chunks)} chunks of len={len(chunks[0])}")
    with multiprocessing.Pool(args.procs) as pool:
        for chunk in tqdm(chunks):
            results = pool.map(f, chunk)
            for url, review in results:
                insert_review(db, url, review)

    db.commit()
    db.close()

    if not args.no_dbt:
        print("\nExecuting DBT test...")
        dbt("test")
        print()

    print("All good!")
