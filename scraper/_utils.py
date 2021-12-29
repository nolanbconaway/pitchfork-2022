import datetime
import time
from pathlib import Path

from selenium import webdriver
from selenium.webdriver.common.by import By

DEFAULT_RETRIES: int = 50
DEFAULT_TIMEOUT: float = 5.0
PAGES_SAVE_PATH: Path = Path("_data/pages/")
REVIEWS_SAVE_PATH: Path = Path("_data/reviews/")
SQLITE_SAVE_PATH: Path = Path("_data/data.sqlite3")
FIRST_BEST_NEW_MUSIC: datetime.datetime = datetime.datetime(2003, 1, 15)

SQL_FILES: dict[str, list[Path]] = {
    "ddl": sorted(list((Path(__file__).parent.resolve() / "sql/ddl").glob("*.sql"))),
    "view": sorted(list((Path(__file__).parent.resolve() / "sql/view").glob("*.sql"))),
    "index": sorted(
        list((Path(__file__).parent.resolve() / "sql/index").glob("*.sql"))
    ),
}


class DriverContext:
    """Context manager for a driver session."""

    def __init__(
        self, headless: bool = False, wait_seconds: float = None, print_: bool = True
    ) -> None:
        """Store settings for the context."""
        self.headless = headless
        self.wait_seconds = wait_seconds or DEFAULT_TIMEOUT
        self.print_ = print_

    def __enter__(self) -> webdriver.Chrome:
        """Create a driver session."""
        options = webdriver.ChromeOptions()
        options.add_argument("window-size=900x600")
        if self.headless:
            options.add_argument("headless")

        self.driver = webdriver.Chrome(options=options)
        self.driver.implicitly_wait(self.wait_seconds)

        # self.console = Console()
        return self

    def __exit__(self, *exc):
        """Make sure we always quit at end."""
        self.driver.quit()

    def get_with_retries(self, url: str, selector: str, retries: int = None) -> str:
        f"""Get a URL and return the source html.

        Wraps with many reties and requires the user to provide a wait css selector.
        Retries the GET request every 5 failed CSS checks, and waits {DEFAULT_TIMEOUT}
        seconds between checks.
        """
        retries = retries or DEFAULT_RETRIES
        if self.print_:
            print(f"Getting {url}...")

        for i in range(retries):
            try:
                if i % 5 == 0:
                    self.driver.get(url)
                    time.sleep(DEFAULT_TIMEOUT)
                self.driver.find_element(By.CSS_SELECTOR, selector)
                break
            except Exception as e:
                if i == (retries - 1):
                    raise
                else:
                    if self.print_:
                        print(f"Excepted on {url}: {str(e)}, retrying...")
                    time.sleep(DEFAULT_TIMEOUT)

        return self.driver.page_source
