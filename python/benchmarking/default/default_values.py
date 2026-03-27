from __future__ import annotations

from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent.parent
REPO_ROOT = BASE_DIR.parent
BENCHMARKING_DIR = BASE_DIR / "benchmarking"

DATA_GEN_OUT_DIR = REPO_ROOT / "data-gen" / "out"
POLARS_OUT_DIR = BENCHMARKING_DIR / "out_polars"

BENCHMARK_WARMUP = 3
BENCHMARK_ITERATIONS = 7
BENCHMARK_DURATION_SECONDS = 10

AIRLINES_INPUT_PATH = DATA_GEN_OUT_DIR / "airlines.parquet"

PARAM = [
    DATA_GEN_OUT_DIR / "20kFlights.parquet",
    DATA_GEN_OUT_DIR / "80kFlights.parquet",
    DATA_GEN_OUT_DIR / "320kFlights.parquet",
    DATA_GEN_OUT_DIR / "1280kFlights.parquet",
    DATA_GEN_OUT_DIR / "5120kFlights.parquet",
    DATA_GEN_OUT_DIR / "20480kFlights.parquet",
]
