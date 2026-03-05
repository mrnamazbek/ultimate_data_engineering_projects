#!/usr/bin/env python3
"""Download a 1-month sample of NYC Taxi trip data (Jan 2024, ~50 MB parquet)."""

import urllib.request
from pathlib import Path

OUTPUT_DIR = Path(__file__).parent / "nyc_taxi"
URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet"


def download() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    dest = OUTPUT_DIR / "yellow_tripdata_2024-01.parquet"
    if dest.exists():
        print(f"Already downloaded: {dest}")
        return
    print(f"Downloading {URL} ...")
    urllib.request.urlretrieve(URL, dest)
    print(f"Saved to {dest}")


if __name__ == "__main__":
    download()
