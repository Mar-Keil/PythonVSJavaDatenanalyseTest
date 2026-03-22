from __future__ import annotations

from threading import Timer
from time import perf_counter
from time import process_time

from polars_src.benchmarking.default.default_functions import write_result
from polars_src.benchmarking.default.default_values import AIRLINES_INPUT_PATH
from polars_src.benchmarking.default.default_values import BENCHMARK_ITERATIONS
from polars_src.benchmarking.default.default_values import BENCHMARK_DURATION_SECONDS
from polars_src.benchmarking.default.default_values import PARAM
from polars_src.benchmarking.default.ram_measurement import RamMeasurement
from polars_src.logic.logic import read_parquet


class ReadBenchmark:
    def run(self) -> None:
        ram = RamMeasurement()
        for path in PARAM:
            time = 0.0
            cpu = 0.0
            ram.reset()

            for i in range(BENCHMARK_ITERATIONS):
                invocations = 0
                invocation_loop = True
                invocation_time = 0.0
                invocation_cpu_time = 0.0

                def stop_invocation() -> None:
                    nonlocal invocation_loop
                    invocation_loop = False

                timer = Timer(BENCHMARK_DURATION_SECONDS, stop_invocation)
                timer.start()
                ram.continue_ram()

                while invocation_loop:
                    invocations += 1

                    start_time = perf_counter()
                    start_cpu = process_time()

                    read_parquet(path)
                    read_parquet(AIRLINES_INPUT_PATH)

                    end_time = perf_counter()
                    end_cpu = process_time()

                    invocation_time += end_time - start_time
                    invocation_cpu_time += end_cpu - start_cpu

                timer.cancel()
                ram.pause()

                time += invocation_time / invocations
                cpu += invocation_cpu_time / invocation_time

            time = time / BENCHMARK_ITERATIONS
            cpu = cpu / BENCHMARK_ITERATIONS

            write_result(path.stem.replace("Flights", ""), "Read", "Time", time, "s/op")
            write_result(path.stem.replace("Flights", ""), "Read", "CPU", cpu, "ratio/op")
            ram.write_results(path.stem.replace("Flights", ""), "Read")