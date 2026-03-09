package com.tablesaw.benchmark.run;

import java.io.IOException;
import java.nio.file.Path;

import com.tablesaw.benchmark.defaults.BenchmarkDefaults;
import org.openjdk.jmh.annotations.*;
import tech.tablesaw.api.Table;

@State(Scope.Benchmark)
public class FilterBenchmarks extends BenchmarkDefaults {

  private Table rawFlights;
  private Table filteredFlights;

  private Path outputDir;

  @Setup(Level.Trial)
  public void setupTrial() {
    rawFlights = io.readParquet(resolveFlightsPath(flightsDataset));
    filteredFlights = logic.filter(rawFlights);
    outputDir = resolveWriteOutputDir("filter");
  }

  @Benchmark
  public Table filter() {
    filteredFlights = logic.filter(rawFlights);
    return filteredFlights;
  }

  @Benchmark
  public void writeFilter() throws IOException {
    Path output = outputDir.resolve(flightsDataset + "Filter.parquet");
    io.writeParquet(filteredFlights, output);
  }
}
