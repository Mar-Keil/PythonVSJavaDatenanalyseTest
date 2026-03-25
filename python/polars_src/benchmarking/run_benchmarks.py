from __future__ import annotations

from polars_src.benchmarking.default.print_csv import PrintCSV
from polars_src.benchmarking.default.time_cpu_measurement import TimeCPUMeasurement
from polars_src.benchmarking.run.read_benchmark import ReadBenchmark


def main() -> None:
    time_measurement = TimeCPUMeasurement(PrintCSV())

    ReadBenchmark().run(time_measurement)


if __name__ == "__main__":
    main()
