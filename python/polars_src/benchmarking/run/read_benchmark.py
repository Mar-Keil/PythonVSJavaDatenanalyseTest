from __future__ import annotations

from time import perf_counter
from time import process_time

from polars_src.benchmarking.default.default_functions import write_result
from polars_src.benchmarking.default.default_values import BENCHMARK_ITERATIONS
from polars_src.benchmarking.default.default_values import PARAM
from polars_src.benchmarking.default.default_values import AIRLINES_INPUT_PATH
from polars_src.logic.logic import read_parquet


class ReadBenchmark:
    def run(self) -> None:
        for path in PARAM:
            time = 0.0
            cpu = 0.0
            for i in range(BENCHMARK_ITERATIONS):

                #Setup
                start_time = perf_counter()
                start_cpu = process_time()

                #Benchmark
                read_parquet(path)
                read_parquet(AIRLINES_INPUT_PATH)

                #Teardown
                end_time = perf_counter()
                end_cpu = process_time()

                time += end_time - start_time
                cpu += (end_cpu - start_cpu) / (end_time - start_time)

            time = time / BENCHMARK_ITERATIONS
            cpu = cpu / BENCHMARK_ITERATIONS

            write_result(path.stem.replace("Flights", ""), "Read", "Time", time, "s/op")
            write_result(path.stem.replace("Flights", ""), "Read", "CPU", cpu, "ratio/op")

