#!/usr/bin/env python3
"""Download the MovieLens 'ml-latest-small' dataset (~1 MB zip, extracted CSV)."""

import urllib.request
import zipfile
from pathlib import Path

OUTPUT_DIR = Path(__file__).parent / "movielens"
URL = "https://files.grouplens.org/datasets/movielens/ml-latest-small.zip"


def download() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    if (OUTPUT_DIR / "ml-latest-small").exists():
        print("Already downloaded.")
        return
    print(f"Downloading {URL} ...")
    data, _ = urllib.request.urlretrieve(URL)
    with zipfile.ZipFile(data) as z:
        z.extractall(OUTPUT_DIR)
    print(f"Extracted to {OUTPUT_DIR}")


if __name__ == "__main__":
    download()
