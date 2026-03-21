from __future__ import annotations

from time import perf_counter

from polars.benchmarking.default_values import AIRLINES_INPUT_PATH, INPUT_PATHS
from polars.logic.logic import read_parquet


class ReadBenchmark:
    def measure_read_time(self, dataset_path, airlines_path) -> float:
        start_time = perf_counter()

        flights = read_parquet(dataset_path)
        airlines = read_parquet(airlines_path)

        end_time = perf_counter()

        return end_time - start_time

    def run(self) -> list[tuple[str, float]]:
        results: list[tuple[str, float]] = []

        for dataset_name, dataset_path in INPUT_PATHS:
            elapsed_time = self.measure_read_time(
                dataset_path=dataset_path,
                airlines_path=AIRLINES_INPUT_PATH,
            )
            results.append((dataset_name, elapsed_time))

        return results
