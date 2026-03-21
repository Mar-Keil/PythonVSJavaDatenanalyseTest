from __future__ import annotations

from time import perf_counter

from polars.benchmarking.default.default_functions import write_result
from polars.benchmarking.default.default_values import BENCHMARK_ITERATIONS
from polars.benchmarking.default.default_values import PARAM
from polars.logic.logic import read_parquet


class ReadBenchmark:
    def run(self) -> None:
        time = 0.0
        for path in PARAM:
            for i in range(BENCHMARK_ITERATIONS):
                start_time = perf_counter()

                read_parquet(path)

                time += perf_counter() - start_time

            time = time / BENCHMARK_ITERATIONS

            write_result(path.stem.replace("Flights", ""), "Read", "Time", time, "s/op")

