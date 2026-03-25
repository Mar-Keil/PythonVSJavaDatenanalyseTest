from __future__ import annotations

from polars_src.benchmarking.default.print_csv import create_csv
from polars_src.benchmarking.run.read_benchmark import ReadBenchmark


def main() -> None:
    create_csv()
    ReadBenchmark().run()

if __name__ == "__main__":
    main()
