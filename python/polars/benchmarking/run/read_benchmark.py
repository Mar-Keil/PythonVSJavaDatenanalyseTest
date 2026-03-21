from __future__ import annotations

from time import perf_counter

from polars.logic.logic import read_parquet


class ReadBenchmark:
    def measure_read_time(self, dataset_path, airlines_path) -> float:
        start_time = perf_counter()

        flights = read_parquet(dataset_path)
        airlines = read_parquet(airlines_path)

        end_time = perf_counter()

        return end_time - start_time

