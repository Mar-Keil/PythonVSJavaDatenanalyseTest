from __future__ import annotations

from threading import Thread
from time import sleep
from psutil import Process

from polars_src.benchmarking.default.default_functions import write_result
from polars_src.benchmarking.default.default_values import TURN_IN_MB

class RamMeasurement:
    def __init__(self) -> None:
        self.process = Process()
        self.running = False
        self.measurements = 0
        self.rss_sum = 0
        self.rss_max = 0
        self.thread: Thread | None = None

    def reset(self) -> None:
        self.measurements = 0
        self.rss_sum = 0
        self.rss_max = 0

    def continue_ram(self) -> None:
        self.running = True
        self.thread = Thread(target=self.measure)
        self.thread.start()

    def measure(self) -> None:
        while self.running:
            rss = self.process.memory_info().rss
            self.measurements += 1
            self.rss_sum += rss

            if rss > self.rss_max:
                self.rss_max = rss

            sleep(0.01)

    def pause(self) -> None:
        self.running = False
        self.thread.join()
        self.thread = None

    def stop(self) -> None:
        self.pause()

    def write_results(self, benchmark_size: str, method: str) -> None:
        rss_avg = self.rss_sum / self.measurements

        write_result(benchmark_size, method, "RAM_AVG", rss_avg / TURN_IN_MB, "MiB")
        write_result(benchmark_size, method, "RAM_MAX", self.rss_max / TURN_IN_MB, "MiB")

        self.reset()