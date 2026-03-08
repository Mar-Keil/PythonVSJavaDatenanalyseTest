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
FLIGHT_NUMBER_MIN = 10_000_000
FLIGHT_NUMBER_MAX_EXCLUSIVE = 100_000_000
BENCHMARK_DATASET_ROWS = [
    25_000,
    100_000,
    400_000,
    1_600_000,
    6_400_000,
    #25_600_000,
]
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

AIRPORT_CODES = [
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


@dataclass(frozen=True)
class GeneratorConfig:
    rows: int
    airlines: int
    seed: int
    reference_date: date
    benchmark_sizes: bool
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
        "--reference-date",
        type=date.fromisoformat,
        default=date(2025, 12, 31),
        help="Reference date (YYYY-MM-DD). Generated flights are always strictly before this date.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=BASE_DIR / "out",
        help="Directory for generated Parquet files.",
    )
    parser.add_argument(
        "--benchmark-sizes",
        dest="benchmark_sizes",
        action="store_true",
        default=True,
        help="Generate benchmark dataset folders from BENCHMARK_DATASET_ROWS (default).",
    )
    parser.add_argument(
        "--single-size",
        dest="benchmark_sizes",
        action="store_false",
        help="Generate only one dataset using --rows in the output-dir root.",
    )
    args = parser.parse_args()

    if not args.benchmark_sizes and args.rows <= 0:
        raise ValueError("--rows must be greater than 0")
    airlines = len(AIRLINE_NAMES) if args.airlines is None else args.airlines

    if airlines <= 0:
        raise ValueError("--airlines must be greater than 0")
    if airlines > len(AIRLINE_NAMES):
        raise ValueError(f"--airlines must be <= {len(AIRLINE_NAMES)}")

    return GeneratorConfig(
        rows=args.rows,
        airlines=airlines,
        seed=args.seed,
        reference_date=args.reference_date,
        benchmark_sizes=args.benchmark_sizes,
        output_dir=args.output_dir,
    )


MAX_FLIGHT_DURATION_MINUTES = 720


def random_flight_date(rng: random.Random, latest_departure_day: date) -> date:
    start = date(2016, 1, 1)
    max_offset = (latest_departure_day - start).days
    if max_offset < 0:
        raise ValueError("latest_departure_day must be on or after 2016-01-01")
    return start + timedelta(days=rng.randint(0, max_offset))


def build_airlines(codes: list[int], seed: int) -> pl.DataFrame:
    rng = random.Random(seed + 1)

    rows = {
        "airline_code": [],
        "airline_name": [],
        "founding_year": [],
        "hub_airport": [],
    }

    for idx, code in enumerate(codes):
        rows["airline_code"].append(code)
        rows["airline_name"].append(AIRLINE_NAMES[idx])
        rows["founding_year"].append(rng.randint(1950, 2020))
        rows["hub_airport"].append(rng.choice(AIRPORT_CODES))

    return pl.DataFrame(rows)


def random_time_minutes(rng: random.Random) -> int:
    return rng.randint(0, 23 * 60 + 59)


def format_time(total_minutes: int, day: date) -> str:
    hours = total_minutes // 60
    minutes = total_minutes % 60
    return f"{hours:02d}:{minutes:02d} {day.strftime('%d.%m.%Y')}"


def arrival_schedule(departure_minutes: int, duration_minutes: int, departure_day: date) -> tuple[int, date]:
    total_arrival_minutes = departure_minutes + duration_minutes
    arrival_minutes = total_arrival_minutes % (24 * 60)
    arrival_day = departure_day + timedelta(days=total_arrival_minutes // (24 * 60))
    return arrival_minutes, arrival_day


def build_flights(rows: int, airline_codes: list[int], seed: int, reference_date: date) -> pl.DataFrame:
    rng = random.Random(seed)
    flight_numbers = range(FLIGHT_NUMBER_MIN, FLIGHT_NUMBER_MIN + rows)
    plane_pool_size = max(50, rows // 5)
    msn_to_model: dict[str, str] = {}
    max_arrival_day_shift = (23 * 60 + 59 + MAX_FLIGHT_DURATION_MINUTES) // (24 * 60)
    latest_departure_day = reference_date - timedelta(days=max_arrival_day_shift + 1)

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
        "flight_distance": [],
    }

    for flight_number in flight_numbers:
        departure_day = random_flight_date(rng, latest_departure_day=latest_departure_day)
        msn = f"MSN{(100_000 + (flight_number % plane_pool_size)):06d}"
        if msn not in msn_to_model:
            msn_to_model[msn] = rng.choice(AIRCRAFT_MODELS)
        departure_airport, arrival_airport = rng.sample(AIRPORT_CODES, 2)
        departure_minutes = random_time_minutes(rng)
        duration_minutes = rng.randint(50, MAX_FLIGHT_DURATION_MINUTES)
        arrival_minutes, arrival_day = arrival_schedule(departure_minutes, duration_minutes, departure_day)

        records["flight_number"].append(f"FL{flight_number}")
        records["msn"].append(msn)
        records["aircraft_model"].append(msn_to_model[msn])
        records["error_free"].append(rng.random() < 0.9)
        records["airline_code"].append(rng.choice(airline_codes))
        records["departure_airport"].append(departure_airport)
        records["arrival_airport"].append(arrival_airport)
        records["departure_time"].append(format_time(departure_minutes, departure_day))
        records["arrival_time"].append(format_time(arrival_minutes, arrival_day))
        # Distance in kilometers (int64), loosely correlated with flight duration.
        records["flight_distance"].append(int(round(duration_minutes * rng.uniform(9.0, 14.0))))

    return pl.DataFrame(records)


def write_dataset(rows: int, config: GeneratorConfig, output_dir: Path) -> None:
    rng = random.Random(config.seed)
    airline_codes = sorted(rng.sample(range(1000, 9999), config.airlines))

    airlines_df = build_airlines(codes=airline_codes, seed=config.seed)
    flights_df = build_flights(
        rows=rows,
        airline_codes=airline_codes,
        seed=config.seed,
        reference_date=config.reference_date,
    )

    output_dir.mkdir(parents=True, exist_ok=True)
    airlines_path = output_dir / "airlines.parquet"
    flights_path = output_dir / "flights.parquet"

    airlines_df.write_parquet(airlines_path, compression="zstd")
    flights_df.write_parquet(flights_path, compression="zstd")

    print(f"Wrote {len(airlines_df)} rows to {airlines_path}")
    print(f"Wrote {len(flights_df)} rows to {flights_path}")


def format_rows_label(rows: int) -> str:
    if rows % 1_000_000 == 0:
        return f"{rows // 1_000_000}m"
    if rows % 1_000 == 0:
        return f"{rows // 1_000}k"
    return str(rows)


def write_airlines_once(config: GeneratorConfig, output_dir: Path) -> None:
    rng = random.Random(config.seed)
    airline_codes = sorted(rng.sample(range(1000, 9999), config.airlines))
    airlines_df = build_airlines(codes=airline_codes, seed=config.seed)
    output_dir.mkdir(parents=True, exist_ok=True)
    airlines_path = output_dir / "airlines.parquet"
    airlines_df.write_parquet(airlines_path, compression="zstd")
    print(f"Wrote {len(airlines_df)} rows to {airlines_path}")


def write_flights_only(rows: int, config: GeneratorConfig, output_dir: Path) -> None:
    rng = random.Random(config.seed)
    airline_codes = sorted(rng.sample(range(1000, 9999), config.airlines))
    flights_df = build_flights(
        rows=rows,
        airline_codes=airline_codes,
        seed=config.seed,
        reference_date=config.reference_date,
    )
    output_dir.mkdir(parents=True, exist_ok=True)
    flights_path = output_dir / f"{format_rows_label(rows)}Flights.parquet"
    flights_df.write_parquet(flights_path, compression="zstd")
    print(f"Wrote {len(flights_df)} rows to {flights_path}")


def main() -> None:
    config = parse_args()
    if config.benchmark_sizes:
        write_airlines_once(config=config, output_dir=config.output_dir)
        for rows in BENCHMARK_DATASET_ROWS:
            write_flights_only(rows=rows, config=config, output_dir=config.output_dir)
    else:
        write_dataset(rows=config.rows, config=config, output_dir=config.output_dir)


if __name__ == "__main__":
    main()
