from __future__ import annotations

from benchmarking.default.default_values import PANDAS_OUT_DIR
from benchmarking.default.print_csv import PrintCSV
from benchmarking.default.time_cpu_measurement import TimeCPUMeasurement
from benchmarking.run_pandas import (
    FilterBenchmark,
    GroupCountBenchmark,
    JoinBenchmark,
    PivotBenchmark,
    ReadBenchmark,
    SortBenchmark,
)


def main() -> None:
    print_csv = PrintCSV(PANDAS_OUT_DIR)

    logic_measurement = TimeCPUMeasurement(print_csv)
    write_measurement = TimeCPUMeasurement(print_csv)

    ReadBenchmark().run(TimeCPUMeasurement(print_csv))
    FilterBenchmark().run(logic_measurement, write_measurement, PANDAS_OUT_DIR)
    SortBenchmark().run(logic_measurement, write_measurement, PANDAS_OUT_DIR)
    PivotBenchmark().run(logic_measurement, write_measurement, PANDAS_OUT_DIR)
    GroupCountBenchmark().run(logic_measurement, write_measurement, PANDAS_OUT_DIR)
    JoinBenchmark().run(logic_measurement, write_measurement, PANDAS_OUT_DIR)


if __name__ == "__main__":
    main()
