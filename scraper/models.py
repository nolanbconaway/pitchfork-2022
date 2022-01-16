"""Use the saved review data to build an analytics-ready SQLite db."""
import datetime
from typing import Union, Any

from bs4 import BeautifulSoup
from pydantic import BaseModel, root_validator, validator


def unique(l: list[Any]) -> list[Any]:
    """Get unique items and retain order. Thx stackoverflow."""
    seen = set()
    seen_add = seen.add
    return [x for x in l if not (x in seen or seen_add(x))]


class Artist(BaseModel):
    name: str
    # some artists have no URL (various, etc)
    url: Union[str, None]

    @validator("url", always=True)
    def check_url_startswith_artist(cls, v):
        if v is not None:
            assert v.startswith("/artists/")
            assert v.endswith("/")
        return v

    @validator("name", always=True)
    def check_name_has_value(cls, v):
        assert v
        return v

    @property
    def artist_id(self) -> str:
        if self.url is None:
            return "various-" + self.name.replace(" ", "-").lower()
        return self.url.lstrip("/artists/").rstrip("/")

    def __hash__(self) -> int:
        return (self.name, self.url).__hash__()


class Tombstone(BaseModel):
    title: str
    release_years: Union[list[int], None]
    score: float
    best_new_music: bool
    best_new_reissue: bool

    @validator("title", always=True)
    def check_title_has_value(cls, v):
        assert v
        return v

    @validator("score", always=True)
    def check_score_bounds(cls, v):
        assert v >= 0 and v <= 10
        return v

    @classmethod
    def from_soup(cls, soup: BeautifulSoup) -> "Tombstone":
        return cls(
            title=cls.get_title(soup),
            release_years=cls.get_release_years(soup),
            score=cls.get_score(soup),
            best_new_music=cls.get_best_new_music(soup),
            best_new_reissue=cls.get_best_new_reissue(soup),
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
    def get_best_new_music(soup: BeautifulSoup) -> list[bool]:
        bnm = soup.find("p", {"class": "bnm-txt"})
        return bnm is not None and "reissue" not in bnm.text.lower()

    @staticmethod
    def get_best_new_reissue(soup: BeautifulSoup) -> bool:
        bnm = soup.find("p", {"class": "bnm-txt"})
        return bnm is not None and "reissue" in bnm.text.lower()


class Review(BaseModel):
    artists: list[Artist]
    body: str
    is_sunday_review: bool
    labels: list[str]
    genres: list[str]
    pub_date: datetime.datetime
    authors: list[str]
    tombstones: list[Tombstone]

    @validator("pub_date", always=True)
    def check_pub_date_value(cls, v):
        assert v >= datetime.datetime(1999, 1, 1)
        return v

    @validator("artists", "authors", "tombstones", always=True)
    def check_has_values(cls, v):
        assert v
        return v

    @root_validator
    def check_body_except_exceptions(cls, values):
        """Make an assertion about the review body.

        Pitchfork hilariously reviewed Shine on by Jet with a video of a chimpanzee
        peeing, so need to except that.

        Pitchfork hilariously reviewed Partie Traumatic by Black Kids with a picture of
        a dog. So need to except that.

        If I find even one more example then I will manage these exceptions better.
        """
        first_title = values["tombstones"][0].title
        first_artist = values["artists"][0].name
        if first_title == "Shine On" and first_artist == "Jet":
            values["body"] = "https://www.youtube.com/watch?v=SvZmRv6U_s0&t=1s"
            return values

        if first_title == "Partie Traumatic" and first_artist == "Black Kids":
            values["body"] = "sorry :-/"
            return values

        assert values["body"]
        return values

    @classmethod
    def from_html(cls, html: str) -> "Review":
        """Create a Review object from an HTML string."""
        soup = BeautifulSoup(html, "lxml")
        return cls(
            artists=cls.get_artists(soup),
            genres=cls.get_genres(soup),
            body=cls.get_review_body(soup),
            is_sunday_review=cls.detect_sunday_review(soup),
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
        return datetime.datetime.fromisoformat(time_["datetime"])

    @classmethod
    def get_release_labels(cls, soup: BeautifulSoup) -> list[str]:
        ul = soup.find("ul", {"class": ["labels-list"]})
        return unique([i.text.strip() for i in ul.findAll("li")])

    @staticmethod
    def get_review_body(soup: BeautifulSoup) -> str:
        div = soup.find("div", {"class": "review-detail__text"})
        ps = []
        for el in div.find_all(["p", "hr"]):
            if el.name == "hr":
                break
            ps.append(el.text.strip())
        return "\n\n".join(ps)

    @classmethod
    def detect_sunday_review(cls, soup: BeautifulSoup) -> bool:
        """Return True if the review is a Sunday review."""
        published_on_sunday = cls.get_pub_date(soup).weekday() == 6
        abstract = soup.find("div", {"class": "review-detail__abstract"}).text.strip()
        return published_on_sunday and abstract.startswith(
            "Each Sunday, Pitchfork takes an in-depth look"
        )

    @staticmethod
    def get_genres(soup: BeautifulSoup) -> list[str]:
        ul = soup.find("ul", {"class": "genre-list"})
        # some don't have genres
        return unique([] if ul is None else [i.text.strip() for i in ul.findAll("li")])

    @staticmethod
    def get_authors(soup: BeautifulSoup) -> list[str]:
        ul = soup.find("ul", {"class": "authors-detail"})
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
            return [
                Tombstone.from_soup(div)
                for div in ul.find_all("div", {"class": "single-album-tombstone"})
            ]
        else:
            div = soup.find("div", {"class": "single-album-tombstone"})
            return [Tombstone.from_soup(div)]

    @property
    def is_standard_review(self) -> bool:
        """Return True if the review is a standard review.

        This is NOT a pitchfork concept, but a rule-based derivation that I made up to
        identify "ordinary" reviews.

        Excludes:
            - Sunday reviews
            - Multi-album reviews
            - Any best new reissue
            - Anything with multiple release years
            - Any review posted long after the release date
        """
        if self.is_sunday_review:
            return False
        if len(self.tombstones) > 1:
            return False

        tombstone = self.tombstones[0]

        if tombstone.best_new_reissue:
            return False

        release_years = tombstone.release_years
        if release_years is None:
            return True

        if len(release_years) > 1:
            return False

        if release_years[0] < (self.pub_date.year - 1):
            # sometimes dec reviews are posted in jan
            return False

        return True
