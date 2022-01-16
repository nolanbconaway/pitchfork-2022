"""Run spot checks on review body content.

There is too much of this and too muchn weird encoding to do in full; so this file 
contains manually configured checks for specific reviews.

This can be added too eternally; so far I have added a few reviews from very different
period of Pitchfork writing. The idea was to first pick reviews from those periods, 
look at the HTML, and make sure I check stuff that looks strange. It uncovered SEVERAL
bugs in the scraper.
"""
import argparse
import sqlite3
from pathlib import Path
from tqdm import tqdm
from ._utils import SQLITE_SAVE_PATH

from dataclasses import dataclass


@dataclass
class Check:
    """A class to configure assertions to make about data.

    These should be compiled MANUALLY, and then added to the `checks` list.
    """

    url: str
    parargraphs: int
    snippets: list[str]

    def do_check(self, cur: sqlite3.Cursor):
        sql = """select body from reviews where review_url = ?"""
        cur.execute(sql, (self.url,))
        body = cur.fetchone()
        assert body is not None, f"{self.url} not found in db."

        body = body[0]

        paragraphs = body.count("\n\n") + 1  # +1 for final paragraph
        assert (
            paragraphs == self.parargraphs
        ), f"Needed {self.parargraphs} paragraphs, got {paragraphs} for {self.url}."

        for snip in self.snippets:
            assert snip in body, f"snip not found in {self.url}: {snip}"


CHECKS: list[Check] = [
    Check(
        url="/reviews/albums/23067-mista-thug-isolation/",
        parargraphs=5,
        snippets=[
            """How does Travis Miller, a noise musician from Richmond, Virginia""",
            """It's a nod to his '90s obsessions""",  # checks for unicode
            """"Mona Lisa Overdrive" sounds like Clams Casino""",  # more unicode
            """It gets real lonely being in your own lane.""",
        ],
    ),
    Check(
        url="/reviews/albums/tonstartssbandht-petunia/",
        parargraphs=5,
        snippets=[
            """No matter how many decades pass since its 1960s heyday""",
            """Now, Tonstartssbandht brush away all the echo and distortion""",
        ],
    ),
    Check(
        url="/reviews/albums/21092-astral-weeks-his-band-and-the-street-choir/",
        parargraphs=9,
        snippets=[
            """Van Morrison released Astral Weeks in November 1968""",
            """separation and nude modeling, not exactly rousing topics""",
        ],
    ),
    Check(
        url="/reviews/albums/14629-expektoration-live/",
        parargraphs=5,
        snippets=[
            """Part of the appeal of a live album is that it's raw, real, truthful""",
            """That's not supervillainy, that's antagonism.""",
        ],
    ),
    Check(
        url="/reviews/albums/7820-already-platinum/",
        parargraphs=5,
        snippets=[
            """Slim Thug is not a complicated man.""",
            """most of their tracks have lost the gleaming, clattering swagger""",
            """Slim sounds perfectly at home on these tracks""",
        ],
    ),
    Check(
        url="/reviews/albums/385-since-i-left-you/",
        parargraphs=8,
        snippets=[
            """Contrary to the beliefs of many middle-American supermarket-goers""",
            """Since I Left You is a remarkably coherent record on all fronts""",
            """Since I Left You is the perfect record for the party""",
        ],
    ),
]


def parse_args() -> argparse.Namespace:
    """Make the parser for the command line arguments."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--in",
        dest="in_",
        type=Path,
        help="The path to save the sqlite database.",
        default=SQLITE_SAVE_PATH,
    )
    args = parser.parse_args()
    return args


if __name__ == "__main__":
    args = parse_args()
    with sqlite3.connect(args.in_) as db:
        cur = db.cursor()
        for check in tqdm(CHECKS):
            check.do_check(cur)
