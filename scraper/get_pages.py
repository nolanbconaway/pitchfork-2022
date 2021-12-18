"""Save review URLs from each Pitchfork page to JSON files for later."""
import argparse
import datetime
import json
import shutil
from pathlib import Path

from bs4 import BeautifulSoup

from ._utils import PAGES_SAVE_PATH, DriverContext


def is_last_page(soup: BeautifulSoup) -> bool:
    """Check if the given soup is the last page of the album reviews."""
    return soup.find("div", {"class": "end-infinite"}) is not None


def get_page_by_number(driver: DriverContext, number: int) -> BeautifulSoup:
    """Get the page with the given number."""
    html = driver.get_with_retries(
        url=f"https://pitchfork.com/reviews/albums/?page={number}",
        selector="#site-content",
    )
    assert "#site-content" in html  # catch weirdness
    return BeautifulSoup(html, "lxml")


def get_reviews_from_page(source: BeautifulSoup) -> list[str]:
    """Get the reviews from the given page."""
    urls = [
        i["href"] for i in source.find_all("a", {"class": "review__link"}, href=True)
    ]
    assert len(urls) > 0  # catch weirdness
    return urls


def parse_args() -> argparse.Namespace:
    """Make the parser for the command line arguments."""
    parser = argparse.ArgumentParser(description="Get the album reviews pages.")
    parser.add_argument(
        "--start", type=int, help="The page at which to start (min=1).", default=1
    )
    parser.add_argument(
        "--end",
        type=int,
        help=(
            "The page at which to end, inclusively."
            + " If not set, or if too high, the last page of results is used."
        ),
        default=None,
    )
    parser.add_argument(
        "--out",
        type=Path,
        help=(
            "The path to save the page data."
            + "Will have file per page which is JSON formatted, like <page>.json."
        ),
        default=PAGES_SAVE_PATH,
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

    args = parser.parse_args()
    if args.start < 1:
        raise ValueError("The start page must be at least 1.")

    return args


if __name__ == "__main__":
    args = parse_args()

    if not args.append:
        shutil.rmtree(args.out, ignore_errors=True)
    args.out.mkdir(exist_ok=True)

    page_num = args.start
    with DriverContext(headless=args.headless) as driver:
        while True:
            if args.end is not None and page_num > args.end:
                print("End Reached!")
                break

            page = get_page_by_number(driver, page_num)
            urls = get_reviews_from_page(page)

            with open(args.out / f"{page_num}.json", "w") as f:
                json.dump(
                    {
                        "page_scrape_ts_utc": datetime.datetime.utcnow().isoformat(),
                        "page": page_num,
                        "urls": urls,
                    },
                    f,
                )

            if is_last_page(page):
                print("Last Page Reached!")
                break
            page_num += 1
