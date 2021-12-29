"""Make some assertions about the SQLite data."""
import argparse
import sqlite3
from pathlib import Path

from ._utils import SQLITE_SAVE_PATH


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


def test_can_select_from_views(db: Path) -> None:
    """Test that the views can be selected from."""
    with sqlite3.connect(db) as conn:
        cur = conn.cursor()
        for name in "reviews_flat", "standard_reviews_flat":
            cur.execute(f"select * from {name}")
            for row in cur:
                assert row


if __name__ == "__main__":
    args = parse_args()
    assert args.in_.exists()

    test_can_select_from_views(args.in_)
