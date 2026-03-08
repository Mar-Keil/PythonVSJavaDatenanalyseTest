package com.tablesaw.benchmark.read;

import com.tablesaw.benchmark.defaults.BenchmarkDefaults;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import tech.tablesaw.api.Table;

@State(Scope.Benchmark)
public class ReadBenchmarks extends BenchmarkDefaults {

  @Benchmark
  public int readDatasets() {
    Table flights = io.readParquet(resolveFlightsPath(flightsDataset));
    Table airlines = io.readParquet(resolveAirlinesPath());
    return flights.rowCount() + airlines.rowCount();
  }
}
