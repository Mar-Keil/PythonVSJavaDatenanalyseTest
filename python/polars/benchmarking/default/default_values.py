from __future__ import annotations

from pathlib import Path


BASE_DIR = Path(__file__).resolve().parent.parent.parent
REPO_ROOT = BASE_DIR.parent.parent

DATA_GEN_OUT_DIR = REPO_ROOT / "data-gen" / "out"
POLARS_OUT_DIR = BASE_DIR / "out"

BENCHMARK_ITERATIONS = 7
BENCHMARK_DURATION_SECONDS = 10

AIRLINES_INPUT_PATH = DATA_GEN_OUT_DIR / "airlines.parquet"

INPUT_PATHS = [
    ("50k", DATA_GEN_OUT_DIR / "50kFlights.parquet"),
    ("200k", DATA_GEN_OUT_DIR / "200kFlights.parquet"),
    ("800k", DATA_GEN_OUT_DIR / "800kFlights.parquet"),
    ("3200k", DATA_GEN_OUT_DIR / "3200kFlights.parquet"),
    ("12800k", DATA_GEN_OUT_DIR / "12800kFlights.parquet"),
]
