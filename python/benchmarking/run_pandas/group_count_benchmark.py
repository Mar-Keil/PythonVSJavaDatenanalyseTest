from __future__ import annotations

from pathlib import Path

from benchmarking.default.default_values import BENCHMARK_ITERATIONS
from benchmarking.default.default_values import PARAM
from benchmarking.default.invocation_loop import InvocationLoop
from benchmarking.default.time_cpu_measurement import TimeCPUMeasurement
from pandas_logic.logic import group_count_dataset
from pandas_logic.logic import read_parquet
from pandas_logic.logic import write_parquet


class GroupCountBenchmark:
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

                    grouped_flights = group_count_dataset(flights)

                    logic_measurement.stop()

                    del grouped_flights

                invocation_loop_logic.cancel()

                grouped_flights = group_count_dataset(flights)

                invocation_loop_write = InvocationLoop()
                invocation_loop_write.start()

                while invocation_loop_write.get_is_looping():
                    write_measurement.start()

                    write_parquet(grouped_flights, output_dir / "group_count")

                    write_measurement.stop()

                invocation_loop_write.cancel()

                del grouped_flights

            logic_measurement.write_results(path.stem.replace("Flights", ""), "GroupCount")
            write_measurement.write_results(path.stem.replace("Flights", ""), "WriteGroupCount")
