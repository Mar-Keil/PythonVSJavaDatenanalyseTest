from __future__ import annotations

from time import perf_counter

from polars_src.benchmarking.default.default_functions import write_result
from polars_src.benchmarking.default.default_values import BENCHMARK_ITERATIONS
from polars_src.benchmarking.default.default_values import PARAM
from polars_src.benchmarking.default.default_values import AIRLINES_INPUT_PATH
from polars_src.logic.logic import read_parquet


class ReadBenchmark:
    def run(self) -> None:
        for path in PARAM:
            time = 0.0
            for i in range(BENCHMARK_ITERATIONS):
                start_time = perf_counter()

                read_parquet(path)
                read_parquet(AIRLINES_INPUT_PATH)

                time += perf_counter() - start_time

            time = time / BENCHMARK_ITERATIONS

            write_result(path.stem.replace("Flights", ""), "Read", "Time", time, "s/op")

