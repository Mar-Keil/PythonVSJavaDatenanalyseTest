from __future__ import annotations

from threading import Timer

from polars_src.benchmarking.default.default_values import AIRLINES_INPUT_PATH
from polars_src.benchmarking.default.default_values import BENCHMARK_DURATION_SECONDS
from polars_src.benchmarking.default.default_values import BENCHMARK_ITERATIONS
from polars_src.benchmarking.default.default_values import PARAM
from polars_src.benchmarking.default.ram_measurement import RamMeasurement
from polars_src.benchmarking.default.time_cpu_measurement import TimeCPUMeasurement
from polars_src.logic.logic import read_parquet


class ReadBenchmark:
    def run(self) -> None:
        time = TimeCPUMeasurement();
        ram = RamMeasurement()
        for path in PARAM:

            for _ in range(BENCHMARK_ITERATIONS):

                # Set up iteration
                invocation_loop = True

                def stop_invocation() -> None:
                    nonlocal invocation_loop
                    invocation_loop = False

                timer = Timer(BENCHMARK_DURATION_SECONDS, stop_invocation)
                timer.start()
                ram.start()

                while invocation_loop:

                    # Set up invocation
                    time.start()

                    # Run benchmark invocation
                    flights = read_parquet(path)
                    airlines = read_parquet(AIRLINES_INPUT_PATH)

                    # Tear down invocation
                    time.stop()
                    del flights
                    del airlines

                # Tear down iteration
                timer.cancel()
                ram.stop()

            
            time.write_results(path.stem.replace("Flights", ""), "Read")
            ram.write_results(path.stem.replace("Flights", ""), "Read")
