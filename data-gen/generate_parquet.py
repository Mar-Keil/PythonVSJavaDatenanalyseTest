#!/usr/bin/env python3
"""Generate reproducible aircraft and airline Parquet datasets."""

from __future__ import annotations

import argparse
import random
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path

import polars as pl

BASE_DIR = Path(__file__).resolve().parent
AIRCRAFT_MODELS = [
    "A220",
    "A320 NEO",
    "A321 NEO ACF",
    "A321 XLR",
    "A330-800",
    "A330-900",
    "A330 MRTT",
    "A350-900",
    "A350-1000",
    "A350-900 ULR",
    "A350F",
    "A380-800",
    "A400M",
    "Eurofighter",
]

AIRLINE_NAMES = [
    "SkyBridge Air",
    "Nordic Wings",
    "Atlantic Horizon",
    "Global Vista Airlines",
    "Blue Meridian",
    "Aurora Flight",
    "Summit Jet",
    "Pacific Orbit",
    "Silver Cloud Air",
    "Helios Airways",
    "Polar Stream",
    "Unity Air",
    "Crescent Skies",
    "Falcon Route",
    "Aero Nexus",
    "MetroAir",
    "Starline Aviation",
    "Vista Regional",
    "Cloudspan",
    "Zenith Airways",
    "NorthGate Air",
    "EastBridge Airlines",
    "WestPoint Aviation",
    "SouthWind Air",
    "Vector Airlines",
    "Horizon Connect",
    "Orbit Regional",
    "Blue Arc Air",
    "Meridian Flight",
    "AeroPrime",
    "TriStar Air",
    "Pioneer Wings",
    "SunRoute",
    "Skyline Express",
    "NextGen Air",
    "Frontier Link",
    "Altitude Airlines",
    "Comet Air",
    "SkyPort",
    "Crosswind Air",
]


@dataclass(frozen=True)
class GeneratorConfig:
    rows: int
    airlines: int
    seed: int
    output_dir: Path


def parse_args() -> GeneratorConfig:
    parser = argparse.ArgumentParser(
        description="Generate aircraft and airline benchmark datasets as Parquet files."
    )
    parser.add_argument("--rows", type=int, default=1000, help="Number of aircraft rows.")
    parser.add_argument(
        "--airlines",
        type=int,
        default=40,
        help="Number of airline rows (distinct airline codes).",
    )
    parser.add_argument("--seed", type=int, default=42, help="Random seed.")
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=BASE_DIR / "out",
        help="Directory for generated Parquet files.",
    )
    args = parser.parse_args()

    if args.rows <= 0:
        raise ValueError("--rows must be greater than 0")
    if args.airlines <= 0:
        raise ValueError("--airlines must be greater than 0")
    if args.airlines > len(AIRLINE_NAMES):
        raise ValueError(f"--airlines must be <= {len(AIRLINE_NAMES)}")
    if args.rows > 90_000:
        raise ValueError("--rows must be <= 90000 to keep MSN 5-digit and unique")

    return GeneratorConfig(
        rows=args.rows,
        airlines=args.airlines,
        seed=args.seed,
        output_dir=args.output_dir,
    )


def random_delivery_date(rng: random.Random) -> date:
    year = rng.randint(2016, 2026)
    start = date(year, 1, 1)
    day_offset = rng.randint(0, 364)
    return start + timedelta(days=day_offset)


def build_airlines(codes: list[int], seed: int) -> pl.DataFrame:
    rng = random.Random(seed + 1)

    rows = {
        "airline_code": [],
        "airline_name": [],
        "founding_year": [],
    }

    for idx, code in enumerate(codes):
        rows["airline_code"].append(code)
        rows["airline_name"].append(AIRLINE_NAMES[idx])
        rows["founding_year"].append(rng.randint(1950, 2020))

    return pl.DataFrame(rows)


def build_aircraft(rows: int, airline_codes: list[int], seed: int) -> pl.DataFrame:
    rng = random.Random(seed)
    msns = rng.sample(range(10_000, 100_000), rows)

    records = {
        "msn": [],
        "aircraft_model": [],
        "delivery_date": [],
        "ready_for_service": [],
        "airline_code": [],
    }

    for msn in msns:
        delivery = random_delivery_date(rng)

        records["msn"].append(msn)
        records["aircraft_model"].append(rng.choice(AIRCRAFT_MODELS))
        records["delivery_date"].append(delivery.strftime("%d.%m.%Y"))
        records["ready_for_service"].append(rng.random() < 0.9)
        records["airline_code"].append(rng.choice(airline_codes))

    return pl.DataFrame(records)


def main() -> None:
    config = parse_args()
    rng = random.Random(config.seed)
    airline_codes = sorted(rng.sample(range(1000, 9999), config.airlines))

    airlines_df = build_airlines(codes=airline_codes, seed=config.seed)
    aircraft_df = build_aircraft(
        rows=config.rows,
        airline_codes=airline_codes,
        seed=config.seed,
    )

    config.output_dir.mkdir(parents=True, exist_ok=True)
    airlines_path = config.output_dir / "airlines.parquet"
    aircraft_path = config.output_dir / "aircraft.parquet"

    airlines_df.write_parquet(airlines_path, compression="zstd")
    aircraft_df.write_parquet(aircraft_path, compression="zstd")

    print(f"Wrote {len(airlines_df)} rows to {airlines_path}")
    print(f"Wrote {len(aircraft_df)} rows to {aircraft_path}")


if __name__ == "__main__":
    main()
