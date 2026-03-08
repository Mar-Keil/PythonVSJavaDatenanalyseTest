package com.tablesaw.benchmark;

import java.io.IOException;
import java.nio.file.Path;
import org.openjdk.jmh.annotations.*;
import tech.tablesaw.api.Table;

@State(Scope.Benchmark)
public class PivotBenchmarks extends BenchmarkDefaults {

  private Table rawFlights;
  private Table pivotedFlights;

  private Path outputDir;
  
  @Setup(Level.Trial)
  public void setupTrial() {
    rawFlights = io.readParquet(resolveFlightsPath(flightsDataset));
    pivotedFlights = logic.pivot(rawFlights);
    outputDir = resolveWriteOutputDir("pivot");
  }

  @Benchmark
  public Table pivot() {
    pivotedFlights = logic.pivot(rawFlights);
    return pivotedFlights;
  }

  @Benchmark
  public int writePivot() throws IOException {
    Path output = outputDir.resolve(flightsDataset + "Pivot.parquet");
    io.writeParquet(pivotedFlights, output);
    return pivotedFlights.rowCount();
  }

}
