#!/usr/bin/env python3
"""Generate reproducible flight and airline Parquet datasets."""

from __future__ import annotations

import argparse
import random
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path

import polars as pl

BASE_DIR = Path(__file__).resolve().parent
AIRCRAFT_MODELS = [
    "A220-100",
    "A220-300",
    "A319neo",
    "A320neo",
    "A321neo",
    "A321LR",
    "A321XLR",
    "A330-800",
    "A330-900",
    "A350-900",
    "A350-1000",
    "A350-900 ULR",
    "A380-800"
]

AIRLINE_NAMES = [
    "Lufthansa",
    "SWISS",
    "Edelweiss Air",
    "Austrian Airlines",
    "Eurowings",
    "Iberia",
    "Air France",   
    "KLM",
    "Condor",
    "Marabu",
    "easyJet",
    "Ryanair",
    "ANA",
    "Delta Air Lines",
    "Icelandair",
    "SAS",
    "Emirates",
    "Qatar Airways",
    "JetBlue",
    "Turkish Airlines",
    "British Airways",
    "Virgin Atlantic",
    "Finnair",
    "Air Canada",
    "United Airlines",
    "American Airlines",
    "Alaska Airlines",
    "Spirit Airlines",
    "Frontier Airlines",
    "WestJet",
    "Aer Lingus",
    "TAP Air Portugal",
    "Vueling",
    "Wizz Air",
    "Norwegian",
    "LOT Polish Airlines",
    "Czech Airlines",
    "Air Baltic",
    "Croatia Airlines",
    "Air Serbia",
    "Olympic Air",
    "Aegean Airlines",
    "Etihad Airways",
    "Saudia",
    "Singapore Airlines",
    "Cathay Pacific",
    "Thai Airways",
    "Malaysia Airlines",
    "Garuda Indonesia",
    "Korean Air",
    "Japan Airlines",

]


@dataclass(frozen=True)
class GeneratorConfig:
    rows: int
    airlines: int
    seed: int
    output_dir: Path


def parse_args() -> GeneratorConfig:
    parser = argparse.ArgumentParser(
        description="Generate flight and airline benchmark datasets as Parquet files."
    )
    parser.add_argument("--rows", type=int, default=1000, help="Number of flight rows.")
    parser.add_argument(
        "--airlines",
        type=int,
        default=None,
        help="Number of airline rows (distinct airline codes). Defaults to all names in AIRLINE_NAMES.",
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
    airlines = len(AIRLINE_NAMES) if args.airlines is None else args.airlines

    if airlines <= 0:
        raise ValueError("--airlines must be greater than 0")
    if airlines > len(AIRLINE_NAMES):
        raise ValueError(f"--airlines must be <= {len(AIRLINE_NAMES)}")
    if args.rows > 900_000:
        raise ValueError("--rows must be <= 900000 to keep flight_number unique")

    return GeneratorConfig(
        rows=args.rows,
        airlines=airlines,
        seed=args.seed,
        output_dir=args.output_dir,
    )


def random_flight_date(rng: random.Random) -> date:
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


def random_time_minutes(rng: random.Random) -> int:
    return rng.randint(0, 23 * 60 + 59)


def format_time_with_date(total_minutes: int, flight_day: date) -> str:
    hours = total_minutes // 60
    minutes = total_minutes % 60
    return f"{minutes:02d}:{hours:02d} {flight_day.strftime('%d.%m.%Y')}"


def build_flights(rows: int, airline_codes: list[int], seed: int) -> pl.DataFrame:
    rng = random.Random(seed)
    flight_numbers = rng.sample(range(100_000, 1_000_000), rows)
    plane_pool_size = max(50, rows // 5)
    msn_to_model: dict[str, str] = {}
    airport_codes = [
        "FRA",
        "MUC",
        "BER",
        "HAM",
        "DUS",
        "ZRH",
        "VIE",
        "CDG",
        "AMS",
        "LHR",
        "MAD",
        "BCN",
        "IST",
        "DXB",
        "DOH",
        "JFK",
        "LAX",
        "ORD",
        "ATL",
        "SIN",
        "HND",
        "ICN",
    ]

    records = {
        "flight_number": [],
        "msn": [],
        "aircraft_model": [],
        "error_free": [],
        "airline_code": [],
        "departure_airport": [],
        "arrival_airport": [],
        "departure_time": [],
        "arrival_time": [],
    }

    for flight_number in flight_numbers:
        flight_date = random_flight_date(rng)
        msn = f"MSN{(100_000 + (flight_number % plane_pool_size)):06d}"
        if msn not in msn_to_model:
            msn_to_model[msn] = rng.choice(AIRCRAFT_MODELS)
        departure_airport, arrival_airport = rng.sample(airport_codes, 2)
        departure_minutes = random_time_minutes(rng)
        flight_duration_minutes = rng.randint(50, 720)
        arrival_minutes = (departure_minutes + flight_duration_minutes) % (24 * 60)

        records["flight_number"].append(f"FL{flight_number}")
        records["msn"].append(msn)
        records["aircraft_model"].append(msn_to_model[msn])
        records["error_free"].append(rng.random() < 0.9)
        records["airline_code"].append(rng.choice(airline_codes))
        records["departure_airport"].append(departure_airport)
        records["arrival_airport"].append(arrival_airport)
        records["departure_time"].append(format_time_with_date(departure_minutes, flight_date))
        records["arrival_time"].append(format_time_with_date(arrival_minutes, flight_date))

    return pl.DataFrame(records)


def main() -> None:
    config = parse_args()
    rng = random.Random(config.seed)
    airline_codes = sorted(rng.sample(range(1000, 9999), config.airlines))

    airlines_df = build_airlines(codes=airline_codes, seed=config.seed)
    flights_df = build_flights(
        rows=config.rows,
        airline_codes=airline_codes,
        seed=config.seed,
    )

    config.output_dir.mkdir(parents=True, exist_ok=True)
    airlines_path = config.output_dir / "airlines.parquet"
    flights_path = config.output_dir / "flights.parquet"

    airlines_df.write_parquet(airlines_path, compression="zstd")
    flights_df.write_parquet(flights_path, compression="zstd")

    print(f"Wrote {len(airlines_df)} rows to {airlines_path}")
    print(f"Wrote {len(flights_df)} rows to {flights_path}")


if __name__ == "__main__":
    main()
