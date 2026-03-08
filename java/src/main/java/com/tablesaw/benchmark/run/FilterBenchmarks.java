package com.tablesaw.benchmark.run;

import java.io.IOException;
import java.nio.file.Path;

import com.tablesaw.benchmark.defaults.BenchmarkDefaults;
import com.tablesaw.benchmark.defaults.ExtraMetrics;
import org.openjdk.jmh.annotations.*;
import tech.tablesaw.api.Table;

@State(Scope.Benchmark)
public class FilterBenchmarks extends BenchmarkDefaults {

  private final long initUsedMb;
  private long realBefore;
  private long cpuBefore;

  private Table rawFlights;
  private Table filteredFlights;

  private Path outputDir;

  public FilterBenchmarks() {
    this.initUsedMb = usedHeapMb();
  }

  @Setup(Level.Trial)
  public void setupTrial() {
    rawFlights = io.readParquet(resolveFlightsPath(flightsDataset));
    filteredFlights = logic.filter(rawFlights);
    outputDir = resolveWriteOutputDir("filter");
  }

  @Setup(Level.Iteration)
  public void setupIteration() {
    realBefore = System.nanoTime();
    cpuBefore = os.getProcessCpuTime();
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

  @TearDown(Level.Iteration)
  public void tearDownIteration(ExtraMetrics x){
    long cpuAfter = os.getProcessCpuTime();
    long realAfter = System.nanoTime();

    x.RAM = usedHeapMb() - initUsedMb;
    x.CPU = (double) (cpuAfter - cpuBefore) / (double) (realAfter - realBefore);
  }
}
