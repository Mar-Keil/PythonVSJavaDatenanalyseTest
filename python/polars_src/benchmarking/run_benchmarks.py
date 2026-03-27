from __future__ import annotations

from polars_src.benchmarking.default.print_csv import PrintCSV
from polars_src.benchmarking.default.time_cpu_measurement import TimeCPUMeasurement
from polars_src.benchmarking.run import (
    ReadBenchmark,
    FilterBenchmark,
    SortBenchmark,
    PivotBenchmark,
    GroupCountBenchmark,
    JoinBenchmark,
)


def main() -> None:
    print_csv = PrintCSV()

    logic_measurement = TimeCPUMeasurement(print_csv)
    write_measurement = TimeCPUMeasurement(print_csv)

    ReadBenchmark().run(TimeCPUMeasurement(print_csv))
    FilterBenchmark().run(logic_measurement, write_measurement)
    SortBenchmark().run(logic_measurement, write_measurement)
    PivotBenchmark().run(logic_measurement, write_measurement)
    GroupCountBenchmark().run(logic_measurement, write_measurement)
    JoinBenchmark().run(logic_measurement, write_measurement)


if __name__ == "__main__":
    main()
