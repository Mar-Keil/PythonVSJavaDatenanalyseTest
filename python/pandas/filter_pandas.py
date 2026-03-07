#!/usr/bin/env python3
"""Minimal dataset loader for Pandas."""

from __future__ import annotations

from pathlib import Path

import pandas as pd

BASE_DIR = Path(__file__).resolve().parent
DATA_GEN_OUT = BASE_DIR.parent.parent / "data-gen" / "out"
FLIGHTS_PATH = DATA_GEN_OUT / "flights.parquet"
AIRLINES_PATH = DATA_GEN_OUT / "airlines.parquet"
OUT_DIR = BASE_DIR / "out"
OUT_PATH = OUT_DIR / "filter.parquet"


def load_datasets() -> tuple[pd.DataFrame, pd.DataFrame]:
    flights = pd.read_parquet(FLIGHTS_PATH)
    airlines = pd.read_parquet(AIRLINES_PATH)
    return flights, airlines


def filter_datasets(flights: pd.DataFrame) -> pd.DataFrame:
    return flights[flights["aircraft_model"] == "A319neo"]


if __name__ == "__main__":
    flights_df, airlines_df = load_datasets()
    filtered_flights_df = filter_datasets(flights_df)
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    filtered_flights_df.to_parquet(OUT_PATH, compression="zstd", index=False)
