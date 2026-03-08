package com.tablesaw.benchmark.run;

import java.io.IOException;
import java.nio.file.Path;

import com.tablesaw.benchmark.defaults.BenchmarkDefaults;
import org.openjdk.jmh.annotations.*;
import tech.tablesaw.api.Table;

@State(Scope.Benchmark)
public class SortBenchmarks extends BenchmarkDefaults {

  private Table rawFlights;
  private Table sortedFlights;

  private Path outputDir;
  
  @Setup(Level.Trial)
  public void setupTrial() {
    rawFlights = io.readParquet(resolveFlightsPath(flightsDataset));
    sortedFlights = logic.sort(rawFlights);
    outputDir = resolveWriteOutputDir("sort");
  }

  @Benchmark
  public Table sort() {
    sortedFlights = logic.sort(rawFlights);
    return sortedFlights;
  }

  @Benchmark
  public int writeSort() throws IOException {
    Path output = outputDir.resolve(flightsDataset + "Sort.parquet");
    io.writeParquet(sortedFlights, output);
    return sortedFlights.rowCount();
  }

}
