from __future__ import annotations

from polars.benchmarking.read_benchmark import ReadBenchmark


def main() -> None:
    read_benchmark = ReadBenchmark()
    print(read_benchmark.run())


if __name__ == "__main__":
    main()
