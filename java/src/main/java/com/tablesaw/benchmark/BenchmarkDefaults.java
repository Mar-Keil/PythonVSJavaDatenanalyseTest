package com.tablesaw.benchmark;

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

  private final Path dataOutDir;
  private final Path writeRootDir;

  @Param({"25k", "100k", "400k", "1600k", "6400k"/*, "25600k"*/})
  protected String flightsDataset;

  public BenchmarkDefaults() {
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

  protected long usedHeapMb() {
    Runtime runtime = Runtime.getRuntime();
    long usedBytes = runtime.totalMemory() - runtime.freeMemory();
    return usedBytes / (1024L * 1024L);
  }
}
