#!/usr/bin/env python3

from __future__ import annotations

from pathlib import Path

import polars as pl

BASE_DIR = Path(__file__).resolve().parent
DATA_GEN_OUT = BASE_DIR.parent.parent / "data-gen" / "out"
FLIGHTS_PATH = DATA_GEN_OUT / "flights.parquet"
AIRLINES_PATH = DATA_GEN_OUT / "airlines.parquet"
OUT_DIR = BASE_DIR / "out"
OUT_PATH = OUT_DIR / "pivot_polars.parquet"


def load_datasets() -> tuple[pl.DataFrame, pl.DataFrame]:
    flights = pl.read_parquet(FLIGHTS_PATH)
    airlines = pl.read_parquet(AIRLINES_PATH)
    return flights, airlines


def pivot_datasets(flights: pl.DataFrame) -> pl.DataFrame:
    return flights.group_by("aircraft_model").agg(
            pl.col("flight_distance").sum().alias("sum_flight_distance")
    )

if __name__ == "__main__":
    flights_df, airlines_df = load_datasets()
    pivot_flights_df = pivot_datasets(flights_df)
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    pivot_flights_df.write_parquet(OUT_PATH, compression="zstd")
