package com.tablesaw.benchmark;

import com.tablesaw.io.TablesawIO;
import com.sun.management.OperatingSystemMXBean;
import java.lang.management.ManagementFactory;
import org.openjdk.jmh.annotations.*;
import tech.tablesaw.api.Table;

@State(Scope.Benchmark)
public class ReadBenchmarks extends BenchmarkDefaults {
  private static final int MB = 1024 * 1024;

  private final OperatingSystemMXBean os;
  private final long initUsedMb;
  private long realBefore;
  private long cpuBefore;

  public ReadBenchmarks() {
    this.os = (OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();
    this.initUsedMb = usedHeapMb();
  }

  @State(Scope.Benchmark)
  public static class ReadState extends DatasetParams {
    protected final TablesawIO io = new TablesawIO();
  }

  @Setup(Level.Iteration)
  public void setupIteration() {
    realBefore = System.nanoTime();
    cpuBefore = os.getProcessCpuTime();
  }

  @Benchmark
  public int readDatasets(ReadState state) {
    Table flights = state.io.loadFlightsByLabel(state.flightsDataset);
    Table airlines = state.io.loadAirlines();
    return flights.rowCount() + airlines.rowCount();
  }

  @TearDown(Level.Iteration)
  public void tearDownIteration(ExtraMetrics x) {
    long cpuAfter = os.getProcessCpuTime();
    long realAfter = System.nanoTime();

    x.RAM = usedHeapMb() - initUsedMb;
    x.CPU = (double) (cpuAfter - cpuBefore) / (double) (realAfter - realBefore);
  }

  private long usedHeapMb() {
    Runtime runtime = Runtime.getRuntime();
    long usedBytes = runtime.totalMemory() - runtime.freeMemory();
    return usedBytes / MB;
  }
}
