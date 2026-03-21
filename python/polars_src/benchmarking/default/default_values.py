from __future__ import annotations

from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent.parent
REPO_ROOT = BASE_DIR.parent.parent

DATA_GEN_OUT_DIR = REPO_ROOT / "data-gen" / "out"
POLARS_OUT_DIR = BASE_DIR / "out"

BENCHMARK_WARMUP = 3
BENCHMARK_ITERATIONS = 7
BENCHMARK_DURATION_SECONDS = 10

AIRLINES_INPUT_PATH = DATA_GEN_OUT_DIR / "airlines.parquet"

PARAM = [
    DATA_GEN_OUT_DIR / "50kFlights.parquet",
    DATA_GEN_OUT_DIR / "200kFlights.parquet",
    DATA_GEN_OUT_DIR / "800kFlights.parquet",
    DATA_GEN_OUT_DIR / "3200kFlights.parquet",
    DATA_GEN_OUT_DIR / "12800kFlights.parquet",
    ]
