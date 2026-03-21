from __future__ import annotations

from polars.benchmarking.default.default_functions import create_csv
from polars.benchmarking.run.read_benchmark import ReadBenchmark


def main() -> None:
    create_csv()
    ReadBenchmark().run()

if __name__ == "__main__":
    main()
