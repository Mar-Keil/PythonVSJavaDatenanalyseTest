package com.tablesaw.benchmark.defaults;

import com.sun.management.OperatingSystemMXBean;
import com.tablesaw.io.TablesawIO;
import com.tablesaw.logic.TablesawLogic;
import java.lang.management.ManagementFactory;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import org.openjdk.jmh.annotations.*;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 3)
@Measurement(iterations = 7)
@Fork(value = 1)
@State(Scope.Benchmark)
public abstract class BenchmarkDefaults {

  protected final TablesawIO io;
  protected final TablesawLogic logic;
  protected final OperatingSystemMXBean os;

  private long realBefore;
  private long cpuBefore;

  private final Path dataOutDir;
  private final Path writeRootDir;

  @Param({"20k", "80k", "320k", "1280k", "5120k", "20480k"})
  protected String flightsDataset;

  protected BenchmarkDefaults() {
    this.io = new TablesawIO();
    this.logic = new TablesawLogic();
    this.os = (OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();

    Path cwd = Path.of("").toAbsolutePath().normalize();
    Path repoRoot = cwd.endsWith("java") ? cwd.getParent() : cwd;
    this.dataOutDir = repoRoot.resolve("data-gen/out");
    this.writeRootDir = repoRoot.resolve("java/out/jmh-write");
  }

  protected Path resolveFlightsPath(String datasetLabel) {
    return dataOutDir.resolve(datasetLabel + "Flights.parquet");
  }

  protected Path resolveAirlinesPath() {
    return dataOutDir.resolve("airlines.parquet");
  }

  protected Path resolveWriteOutputDir(String benchmarkName) {
    return writeRootDir.resolve(benchmarkName);
  }

  @Setup(Level.Iteration)
  public void setupIterationMetrics() {
    realBefore = System.nanoTime();
    cpuBefore = os.getProcessCpuTime();
  }

  @TearDown(Level.Iteration)
  public void tearDownIterationMetrics(ExtraMetrics metrics) {
    long cpuAfter = os.getProcessCpuTime();
    long realAfter = System.nanoTime();
    metrics.CPU = (double) (cpuAfter - cpuBefore) / (double) (realAfter - realBefore);
  }
}
