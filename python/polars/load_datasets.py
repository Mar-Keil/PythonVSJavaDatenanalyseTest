#!/usr/bin/env python3
"""Minimal dataset loader for Polars."""

from __future__ import annotations

from pathlib import Path

import polars as pl

BASE_DIR = Path(__file__).resolve().parent
DATA_GEN_OUT = BASE_DIR.parent.parent / "data-gen" / "out"
FLIGHTS_PATH = DATA_GEN_OUT / "flights.parquet"
AIRLINES_PATH = DATA_GEN_OUT / "airlines.parquet"


def load_datasets() -> tuple[pl.DataFrame, pl.DataFrame]:
    flights = pl.read_parquet(FLIGHTS_PATH)
    airlines = pl.read_parquet(AIRLINES_PATH)
    return flights, airlines


if __name__ == "__main__":
    flights_df, airlines_df = load_datasets()
    print(f"Flights: rows={flights_df.height}, cols={flights_df.width}")
    print(f"Airlines: rows={airlines_df.height}, cols={airlines_df.width}")
