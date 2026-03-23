package com.tablesaw.benchmark.run;

import java.io.IOException;
import java.nio.file.Path;

import com.tablesaw.benchmark.defaults.BenchmarkDefaults;
import org.openjdk.jmh.annotations.*;
import tech.tablesaw.api.Table;

@State(Scope.Benchmark)
public class JoinBenchmarks extends BenchmarkDefaults {

  private Table rawFlights;
  private Table rawAirlines;
  private Table joinedFlights;

  private Path outputDir;
  
  @Setup(Level.Trial)
  public void setupTrial() {
    rawFlights = logic.readParquet(resolveFlightsPath(flightsDataset));
    rawAirlines = logic.readParquet(resolveAirlinesPath());
    joinedFlights = logic.join(rawFlights, rawAirlines);
    outputDir = resolveWriteOutputDir("join");
  }

  @Benchmark
  public Table join() {
    joinedFlights = logic.join(rawFlights, rawAirlines);
    return joinedFlights;
  }

  @Benchmark
  public int writeJoin() throws IOException {
    Path output = outputDir.resolve(flightsDataset + "Join.parquet");
    logic.writeParquet(joinedFlights, output);
    return joinedFlights.rowCount();
  }

}
