package com.tablesaw.benchmark.run;

import java.io.IOException;
import java.nio.file.Path;

import com.tablesaw.benchmark.defaults.BenchmarkDefaults;
import org.openjdk.jmh.annotations.*;
import tech.tablesaw.api.Table;

@State(Scope.Benchmark)
public class GroupCountBenchmarks extends BenchmarkDefaults {

  private Table rawFlights;
  private Table groupedFlights;

  private Path outputDir;
  
  @Setup(Level.Trial)
  public void setupTrial() {
    rawFlights = io.readParquet(resolveFlightsPath(flightsDataset));
    groupedFlights = logic.groupCount(rawFlights);
    outputDir = resolveWriteOutputDir("groupCount");
  }

  @Benchmark
  public Table groupCount() {
    groupedFlights = logic.groupCount(rawFlights);
    return groupedFlights;
  }

  @Benchmark
  public int writeGroupCount() throws IOException {
    Path output = outputDir.resolve(flightsDataset + "GroupCount.parquet");
    io.writeParquet(groupedFlights, output);
    return groupedFlights.rowCount();
  }
}
