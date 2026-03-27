from __future__ import annotations

from pathlib import Path

from benchmarking.default.default_values import BENCHMARK_ITERATIONS
from benchmarking.default.default_values import PARAM
from benchmarking.default.invocation_loop import InvocationLoop
from benchmarking.default.time_cpu_measurement import TimeCPUMeasurement
from polars_logic.logic import pivot_dataset
from polars_logic.logic import read_parquet
from polars_logic.logic import write_parquet


class PivotBenchmark:
    def run(
            self,
            logic_measurement: TimeCPUMeasurement,
            write_measurement: TimeCPUMeasurement,
            output_dir: Path,
    ) -> None:
        for path in PARAM:
            flights = read_parquet(path)

            for i in range(BENCHMARK_ITERATIONS):
                invocation_loop_logic = InvocationLoop()
                invocation_loop_logic.start()

                while invocation_loop_logic.get_is_looping():
                    logic_measurement.start()

                    pivoted_flights = pivot_dataset(flights)

                    logic_measurement.stop()

                    del pivoted_flights

                invocation_loop_logic.cancel()

                pivoted_flights = pivot_dataset(flights)

                invocation_loop_write = InvocationLoop()
                invocation_loop_write.start()

                while invocation_loop_write.get_is_looping():
                    write_measurement.start()

                    write_parquet(pivoted_flights, output_dir / "pivot")

                    write_measurement.stop()

                invocation_loop_write.cancel()

                del pivoted_flights

            logic_measurement.write_results(path.stem.replace("Flights", ""), "Pivot")
            write_measurement.write_results(path.stem.replace("Flights", ""), "WritePivot")
