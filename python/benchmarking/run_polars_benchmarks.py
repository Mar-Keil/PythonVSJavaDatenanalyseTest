from __future__ import annotations

from pathlib import Path

from benchmarking.default.default_values import POLARS_OUT_DIR
from benchmarking.default.print_csv import PrintCSV
from benchmarking.default.time_cpu_measurement import TimeCPUMeasurement
from benchmarking.run_polars import (
    ReadBenchmark,
    FilterBenchmark,
    SortBenchmark,
    PivotBenchmark,
    GroupCountBenchmark,
    JoinBenchmark,
)


def main() -> None:
    output_dir = Path(POLARS_OUT_DIR)
    print_csv = PrintCSV(output_dir)

    logic_measurement = TimeCPUMeasurement(print_csv)
    write_measurement = TimeCPUMeasurement(print_csv)

    ReadBenchmark().run(TimeCPUMeasurement(print_csv))
    FilterBenchmark().run(logic_measurement, write_measurement, output_dir)
    SortBenchmark().run(logic_measurement, write_measurement, output_dir)
    PivotBenchmark().run(logic_measurement, write_measurement, output_dir)
    GroupCountBenchmark().run(logic_measurement, write_measurement, output_dir)
    JoinBenchmark().run(logic_measurement, write_measurement, output_dir)


if __name__ == "__main__":
    main()
