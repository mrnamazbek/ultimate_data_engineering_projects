# Data Directory

Contains scripts to download sample datasets used in the projects.

## Datasets

| Dataset | Script | License | Size | Format | Notes |
|---------|--------|---------|------|--------|-------|
| NYC Taxi (Jan 2024) | `download_nyc_taxi.py` | NYCTLC Public | ~50 MB | Parquet | Trip records; numeric features, no PII after stripping |
| MovieLens Small | `download_movielens.py` | GroupLens Non-commercial | ~1 MB | CSV | 100 k ratings, 9 k movies; widely used for recommendation |

## Usage

```bash
python data/download_nyc_taxi.py
python data/download_movielens.py
```

## Dataset selection criteria

| Criterion | NYC Taxi | MovieLens |
|-----------|----------|-----------|
| License | Open (public domain) | Non-commercial research |
| Size | Manageable 1-month slice | Small (< 2 MB) |
| Format | Parquet (preferred) | CSV (convertible) |
| Representativeness | Real production traffic data | Broad user-item rating matrix |
| Labels available | Implicit (trip = conversion) | Explicit ratings 0.5–5 |

> **Note**: Criteo and CommonCrawl samples are very large (≥ tens of GB).
> For local dev use synthetic data or the smaller alternatives above.
