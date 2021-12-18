"""Use the saved review data to build an analytics-ready SQLite db."""
import argparse
import datetime
import gzip
import json
import multiprocessing
import random
import sqlite3
from pathlib import Path
from typing import Any, Generator, Iterable, Union

from bs4 import BeautifulSoup
from pydantic import BaseModel
from tqdm import tqdm

from ._utils import REVIEWS_SAVE_PATH, SQL_FILES, SQLITE_SAVE_PATH


def unique(l: list[Any]) -> list[Any]:
    """Get unique items and retain order. Thx stackoverflow."""
    seen = set()
    seen_add = seen.add
    return [x for x in l if not (x in seen or seen_add(x))]


def chunker(seq: Iterable, size: int) -> Generator:
    """Thx stackoverfow."""
    return (seq[pos : pos + size] for pos in range(0, len(seq), size))


def parse_args() -> argparse.Namespace:
    """Make the parser for the command line arguments."""
    parser = argparse.ArgumentParser(description=__doc__)
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
        "--sample",
        type=int,
        help="Run a random sample of files. Also useflul for debugging.",
    )
    parser.add_argument(
        "--procs",
        type=lambda x: multiprocessing.cpu_count() if x == "max" else int(x),
        default=1,
        help="Run a random sample of files. Also useflul for debugging.",
    )
    args = parser.parse_args()
    return args


class Artist(BaseModel):
    name: str
    url: Union[str, None]

    @property
    def artist_id(self) -> str:
        if self.url is None:
            return "various-" + self.name.replace(" ", "-").lower()
        return self.url.lstrip("/artists/")

    def __hash__(self) -> int:
        return (self.name, self.url).__hash__()


class Tombstone(BaseModel):
    title: str
    release_years: Union[list[int], None]
    score: float
    bnm: bool

    @classmethod
    def from_soup(cls, soup: BeautifulSoup) -> "Tombstone":
        return cls(
            title=cls.get_title(soup),
            release_years=cls.get_release_years(soup),
            score=cls.get_score(soup),
            bnm=cls.get_bnm(soup),
        )

    @staticmethod
    def get_title(soup: BeautifulSoup) -> str:
        span = soup.find("h1", {"class": "single-album-tombstone__review-title"})
        assert span is not None
        return span.text.strip()

    @staticmethod
    def get_release_years(soup: BeautifulSoup) -> list[int]:
        span = soup.find("span", {"class": "single-album-tombstone__meta-year"})
        assert span is not None
        span_text = span.text.replace("•", "").strip()

        # some reviews do not publish a release date
        if not span_text:
            return None
        return unique([int(i.split()[-1]) for i in span_text.split("/")])

    @staticmethod
    def get_score(soup: BeautifulSoup) -> list[float]:
        span = soup.find("span", {"class": "score"})
        assert span is not None
        return float(span.text.strip())

    @staticmethod
    def get_bnm(soup: BeautifulSoup) -> list[bool]:
        return soup.find("p", {"class": "bnm-txt"}) is not None


class Review(BaseModel):
    is_multi_review: bool
    artists: list[Artist]
    body: str
    labels: list[str]
    genres: list[str]
    pub_date: datetime.datetime
    authors: list[str]
    tombstones: list[Tombstone]

    @classmethod
    def from_html(cls, html: str) -> "Review":
        """Create a Review object from an HTML string."""
        soup = BeautifulSoup(html, "lxml")
        return cls(
            is_multi_review=cls.check_multi_review(soup),
            artists=cls.get_artists(soup),
            genres=cls.get_genres(soup),
            body=cls.get_review_body(soup),
            labels=cls.get_release_labels(soup),
            pub_date=cls.get_pub_date(soup),
            authors=cls.get_authors(soup),
            tombstones=cls.get_tombstones(soup),
        )

    @staticmethod
    def check_multi_review(soup: BeautifulSoup) -> bool:
        """Return True if the review is for a multi-album release."""
        return soup.find("div", {"class": "multi-tombstone-widget"}) is not None

    @staticmethod
    def get_artists(soup: BeautifulSoup) -> list[Artist]:
        ul = soup.find("ul", {"class": ["artist-list"]})
        assert ul is not None
        return unique(
            [
                Artist(
                    url=li.find("a")["href"] if li.find("a") else None,
                    name=li.text.strip(),
                )
                for li in ul.findAll("li")
            ]
        )

    @staticmethod
    def get_pub_date(soup: BeautifulSoup) -> datetime.datetime:
        time_ = soup.find("time", {"class": "pub-date"})
        assert time_ is not None
        return datetime.datetime.fromisoformat(time_["datetime"])

    @classmethod
    def get_release_labels(cls, soup: BeautifulSoup) -> list[str]:
        ul = soup.find("ul", {"class": ["labels-list"]})
        assert ul is not None
        return unique([i.text.strip() for i in ul.findAll("li")])

    @staticmethod
    def get_review_body(soup: BeautifulSoup) -> str:
        div = soup.find("div", {"class": "review-body"}).find(
            "div", {"class": "contents"}
        )
        assert div is not None
        # they end the article with a hr tag and then add some junk.
        ps = []
        for el in div.find_all(["p", "hr"], recursive=False):
            if el.name == "hr":
                break
            ps.append(el.text.strip())

        return "\n\n".join(ps)

    @staticmethod
    def get_genres(soup: BeautifulSoup) -> list[str]:
        ul = soup.find("ul", {"class": "genre-list"})
        # some don't have genres
        return unique([] if ul is None else [i.text.strip() for i in ul.findAll("li")])

    @staticmethod
    def get_authors(soup: BeautifulSoup) -> list[str]:
        ul = soup.find("ul", {"class": "authors-detail"})
        assert ul is not None
        return unique(
            [
                i.text.strip()
                for i in ul.findAll("a", {"class": "authors-detail__display-name"})
            ]
        )

    @classmethod
    def get_tombstones(cls, soup: BeautifulSoup) -> list[Tombstone]:
        """Get the tombstone metadata, with multi album logic."""
        if cls.check_multi_review(soup):
            ul = soup.find("ul", {"class": "review-tombstones"})
            assert ul is not None
            return [
                Tombstone.from_soup(div)
                for div in ul.find_all("div", {"class": "single-album-tombstone"})
            ]
        else:
            div = soup.find("div", {"class": "single-album-tombstone"})
            assert div is not None
            return [Tombstone.from_soup(div)]


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
        # db.commit()
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
                tombstone.bnm,
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

    if args.out.exists():
        args.out.unlink()

    review_jsons = (
        tuple(args.in_.glob("*.json.gz")) if not args.single else [args.single]
    )

    if args.sample is not None:
        review_jsons = random.sample(review_jsons, args.sample)

    # shared across later lines. idc about closing it, this is sqlite.
    db = sqlite3.connect(args.out, timeout=10000, check_same_thread=False)

    # build tables
    for fpath in SQL_FILES["ddl"]:
        db.execute(fpath.read_text())

    def f(fpath: Path):
        with gzip.open(fpath, "rb") as f:
            json_data = json.load(f)
        return json_data["url"], Review.from_html(json_data["html"])

    chunks = list(chunker(review_jsons, min(1000, len(review_jsons))))

    print(f"Multiprocessing on {len(chunks)} chunks of len={len(chunks[0])}")
    with multiprocessing.Pool(args.procs) as pool:
        for chunk in tqdm(chunks):
            results = pool.map(f, chunk)
            for url, review in results:
                insert_review(db, url, review)

    # indices/views
    for fpath in SQL_FILES["index"] + SQL_FILES["view"]:
        db.execute(fpath.read_text())

    db.commit()
    db.close()
