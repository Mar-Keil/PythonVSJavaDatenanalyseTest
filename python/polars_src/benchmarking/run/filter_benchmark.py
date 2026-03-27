from __future__ import annotations

from polars_src.benchmarking.default.default_values import BENCHMARK_ITERATIONS
from polars_src.benchmarking.default.default_values import PARAM
from polars_src.benchmarking.default.default_values import POLARS_OUT_DIR
from polars_src.benchmarking.default.invocation_loop import InvocationLoop
from polars_src.benchmarking.default.time_cpu_measurement import TimeCPUMeasurement
from polars_src.logic.logic import read_parquet
from polars_src.logic.logic import filter_dataset
from polars_src.logic.logic import write_parquet



class FilterBenchmark:
    def run(self, logic_measurement: TimeCPUMeasurement, wirte_measurement: TimeCPUMeasurement) -> None:
        for path in PARAM:

            flights = read_parquet(path)

            for i in range(BENCHMARK_ITERATIONS):
                invocation_loop_logic = InvocationLoop()
                invocation_loop_logic.start()

                while invocation_loop_logic.get_is_looping():
                    logic_measurement.start()

                    filterd = filter_dataset(flights)

                    logic_measurement.stop()

                    del filterd

                invocation_loop_logic.cancel()

                filterd = filter_dataset(flights)

                invocation_loop_write = InvocationLoop()
                invocation_loop_write.start()

                while invocation_loop_write.get_is_looping():
                    wirte_measurement.start()

                    write_parquet(filterd, POLARS_OUT_DIR / "filter")

                    wirte_measurement.stop()

                invocation_loop_write.cancel()

                del filterd

            logic_measurement.write_results(path.stem.replace("Flights", ""), "Filter")
            wirte_measurement.write_results(path.stem.replace("Flights", ""), "WriteFilter")
