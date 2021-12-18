"""Use the saved page data to obtain reviews."""
import argparse
import datetime
import gzip
import json
import shutil
from pathlib import Path

from bs4 import BeautifulSoup
from tqdm import tqdm

from ._utils import PAGES_SAVE_PATH, REVIEWS_SAVE_PATH, DriverContext

BASE_URL = "https://pitchfork.com"


def get_review_html(driver: DriverContext, path: str) -> str:
    """Return the page bocy from the url"""
    html = driver.get_with_retries(url=f"{BASE_URL}{path}", selector=".review-body")
    content = BeautifulSoup(html, "lxml").find("div", {"id": "site-content"})
    assert content is not None, "Could not find review body"
    return str(content)


def parse_args() -> argparse.Namespace:
    """Make the parser for the command line arguments."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--in",
        dest="in_",
        type=Path,
        help="The path to the saved pages data.",
        default=PAGES_SAVE_PATH,
    )
    parser.add_argument(
        "--out",
        type=Path,
        help=(
            "The path to save the review data."
            + "Will have file per review which is gzipped and JSON formatted."
        ),
        default=REVIEWS_SAVE_PATH,
    )
    parser.add_argument(
        "--headless",
        help="Option to run the scraper headless",
        action="store_true",
    )
    parser.add_argument(
        "--append",
        help="Option to not delete the existing data.",
        action="store_true",
    )
    parser.add_argument(
        "--new-only",
        help="Option to skip the scrape for review files already in --out.",
        action="store_true",
    )
    args = parser.parse_args()
    return args


if __name__ == "__main__":
    args = parse_args()

    assert args.in_.exists()
    if not args.append:
        shutil.rmtree(args.out, ignore_errors=True)
    args.out.mkdir(exist_ok=True)

    urls = [
        url
        for fpath in args.in_.glob("*.json")
        for url in json.loads(fpath.read_text())["urls"]
    ]

    with DriverContext(headless=args.headless, print_=False) as driver:
        for url in tqdm(urls):
            filename = args.out / f"""{url.replace('/', '__').strip('_')}.json.gz"""

            # skip if not new and not replacing
            if args.new_only and filename.exists():
                continue

            review_data = dict(
                url=url,
                review_scrape_ts_utc=datetime.datetime.utcnow().isoformat(),
                html=get_review_html(driver, url),
            )

            with gzip.open(filename, "wb") as f:
                f.write(json.dumps(review_data).encode())
