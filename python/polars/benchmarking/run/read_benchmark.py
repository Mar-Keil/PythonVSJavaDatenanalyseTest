from __future__ import annotations

from threading import Event, Timer
from time import perf_counter

from polars.benchmarking.default.default_values import BENCHMARK_DURATION_SECONDS
from polars.logic.logic import read_parquet


class ReadBenchmark:
    def measure(
        self,
        airlines_path
    ) -> none:







        stop_event = Event()
        timer = Timer(duration_seconds, stop_event.set)
        durations: list[float] = []

        timer.start()
        try:
            while not stop_event.is_set():
                start_time = perf_counter()

                flights = read_parquet(dataset_path)
                airlines = read_parquet(airlines_path)

                end_time = perf_counter()

                durations.append(end_time - start_time)
        finally:
            timer.cancel()

        return durations
