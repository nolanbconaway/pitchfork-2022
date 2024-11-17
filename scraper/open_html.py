"""A quick HTML opener for debugging.

This runs on my ubuntu desktop; maybe not on what you have.
"""
import argparse
import gzip
import json
import subprocess
import tempfile
import time
from pathlib import Path

from bs4 import BeautifulSoup


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("in_", type=Path)
    args = parser.parse_args()
    return args


if __name__ == "__main__":
    args = parse_args()

    assert args.in_.exists()
    with gzip.open(args.in_, "rb") as f:
        html = json.load(f)["html"]

    with tempfile.NamedTemporaryFile(mode="w", suffix=".html") as f:
        soup = BeautifulSoup(html, "lxml")
        f.write(html)
        subprocess.call(["firefox", f.name])
        time.sleep(1)
