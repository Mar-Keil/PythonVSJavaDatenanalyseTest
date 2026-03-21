from __future__ import annotations

from pathlib import Path


BASE_DIR = Path(__file__).resolve().parent
REPO_ROOT = BASE_DIR.parent.parent

DATA_GEN_OUT_DIR = REPO_ROOT / "data-gen" / "out"
POLARS_OUT_DIR = BASE_DIR / "out"

AIRLINES_INPUT_PATH = DATA_GEN_OUT_DIR / "airlines.parquet"

BENCHMARK_ITERATIONS = 7

INPUT_PATHS = [
    ("50k", DATA_GEN_OUT_DIR / "50kFlights.parquet"),
    ("200k", DATA_GEN_OUT_DIR / "200kFlights.parquet"),
    ("800k", DATA_GEN_OUT_DIR / "800kFlights.parquet"),
    ("3200k", DATA_GEN_OUT_DIR / "3200kFlights.parquet"),
    ("12800k", DATA_GEN_OUT_DIR / "12800kFlights.parquet"),
]

OUTPUT_PATHS = [
    ("filter", POLARS_OUT_DIR / "filter_polars.parquet"),
    ("pivot", POLARS_OUT_DIR / "pivot_polars.parquet"),
    ("group_count", POLARS_OUT_DIR / "groupCount_polars.parquet"),
    ("join", POLARS_OUT_DIR / "join_polars.parquet"),
    ("sort", POLARS_OUT_DIR / "sort_polars.parquet"),
]
