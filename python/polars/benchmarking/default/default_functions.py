import csv

from polars.benchmarking.default.default_values import POLARS_OUT_DIR

CSV_PATH = POLARS_OUT_DIR / "benchmark_results.csv"


def create_csv() -> None:
    with CSV_PATH.open("w", newline="", encoding="utf-8") as csv_file:
        writer = csv.writer(csv_file)
        writer.writerow(["benchmark_size", "method", "category", "score", "unit"])


def write_result(
        benchmark_size: str,
        method: str,
        category: str,
        score: float,
        unit: str,
) -> None:
    with CSV_PATH.open("a", newline="", encoding="utf-8") as csv_file:
        writer = csv.writer(csv_file)
        writer.writerow([benchmark_size, method, category, score, unit])
