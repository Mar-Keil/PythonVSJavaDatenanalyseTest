from __future__ import annotations

from polars_src.benchmarking.default.default_values import AIRLINES_INPUT_PATH
from polars_src.benchmarking.default.default_values import BENCHMARK_ITERATIONS
from polars_src.benchmarking.default.default_values import PARAM
from polars_src.benchmarking.default.invocation_loop import InvocationLoop
from polars_src.benchmarking.default.time_cpu_measurement import TimeCPUMeasurement
from polars_src.logic.logic import read_parquet


class ReadBenchmark:
    def run(self, time_measurement: TimeCPUMeasurement) -> None:
        for path in PARAM:

            for i in range(BENCHMARK_ITERATIONS):
                invocation_loop = InvocationLoop()
                invocation_loop.start()

                while invocation_loop.get_is_looping():
                    time_measurement.start()

                    flights = read_parquet(path)
                    airlines = read_parquet(AIRLINES_INPUT_PATH)

                    time_measurement.stop()

                    del flights
                    del airlines

                invocation_loop.cancel()

            time_measurement.write_results(path.stem.replace("Flights", ""), "Read")
