#!/usr/bin/env python3

from __future__ import annotations

from pathlib import Path

import pandas as pd

BASE_DIR = Path(__file__).resolve().parent
DATA_GEN_OUT = BASE_DIR.parent.parent / "data-gen" / "out"
FLIGHTS_PATH = DATA_GEN_OUT / "flights.parquet"
OUT_DIR = BASE_DIR / "out"
OUT_PATH = OUT_DIR / "pivot_pandas.parquet"


def load_dataset() -> pd.DataFrame:
    return pd.read_parquet(FLIGHTS_PATH)


def pivot_datasets(flights: pd.DataFrame) -> pd.DataFrame:
    return (
        flights.groupby("aircraft_model", as_index=False, sort=False)["flight_distance"]
        .sum()
        .rename(columns={"flight_distance": "sum_flight_distance"})
    )


if __name__ == "__main__":
    flights_df = load_dataset()
    pivot_flights_df = pivot_datasets(flights_df)
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    pivot_flights_df.to_parquet(OUT_PATH, compression="zstd", index=False)
