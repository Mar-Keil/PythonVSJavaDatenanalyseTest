from __future__ import annotations

from polars_src.benchmarking.default.default_values import AIRLINES_INPUT_PATH
from polars_src.benchmarking.default.default_values import BENCHMARK_ITERATIONS
from polars_src.benchmarking.default.default_values import PARAM
from polars_src.benchmarking.default.default_values import POLARS_OUT_DIR
from polars_src.benchmarking.default.invocation_loop import InvocationLoop
from polars_src.benchmarking.default.time_cpu_measurement import TimeCPUMeasurement
from polars_src.logic.logic import join_dataset
from polars_src.logic.logic import read_parquet
from polars_src.logic.logic import write_parquet


class JoinBenchmark:
    def run(self, logic_measurement: TimeCPUMeasurement, write_measurement: TimeCPUMeasurement) -> None:
        airlines = read_parquet(AIRLINES_INPUT_PATH)

        for path in PARAM:
            flights = read_parquet(path)

            for i in range(BENCHMARK_ITERATIONS):
                invocation_loop_logic = InvocationLoop()
                invocation_loop_logic.start()

                while invocation_loop_logic.get_is_looping():
                    logic_measurement.start()

                    joined_flights = join_dataset(flights, airlines)

                    logic_measurement.stop()

                    del joined_flights

                invocation_loop_logic.cancel()

                joined_flights = join_dataset(flights, airlines)

                invocation_loop_write = InvocationLoop()
                invocation_loop_write.start()

                while invocation_loop_write.get_is_looping():
                    write_measurement.start()

                    write_parquet(joined_flights, POLARS_OUT_DIR / "join")

                    write_measurement.stop()

                invocation_loop_write.cancel()

                del joined_flights

            logic_measurement.write_results(path.stem.replace("Flights", ""), "Join")
            write_measurement.write_results(path.stem.replace("Flights", ""), "WriteJoin")
