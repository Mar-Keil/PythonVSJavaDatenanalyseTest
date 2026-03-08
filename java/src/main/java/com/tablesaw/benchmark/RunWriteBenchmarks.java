package com.tablesaw.benchmark;

import com.tablesaw.io.TablesawIO;
import com.tablesaw.logic.TablesawLogic;
import com.sun.management.OperatingSystemMXBean;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import org.openjdk.jmh.annotations.*;
import tech.tablesaw.api.Table;

@State(Scope.Benchmark)
public class RunWriteBenchmarks extends BenchmarkDefaults {
  private static final int MB = 1024 * 1024;

  private final OperatingSystemMXBean os;
  private final long initUsedMb;
  private long realBefore;
  private long cpuBefore;

  public RunWriteBenchmarks() {
    this.os = (OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();
    this.initUsedMb = usedHeapMb();
  }

  @State(Scope.Benchmark)
  public static class BenchmarkState extends DatasetParams {
    protected final TablesawIO io = new TablesawIO();
    protected final TablesawLogic logic = new TablesawLogic();
    protected final List<Path> outputsToCleanup = new ArrayList<>();

    protected Table flights;
    protected Table airlines;

    private Table filtered;
    private Table pivoted;
    private Table groupedCount;
    private Table sorted;
    private Table joined;

    private Path outputDir;
    private final AtomicLong sequence = new AtomicLong();

    @Setup(Level.Trial)
    public void setup() {
      flights = io.loadFlightsByLabel(flightsDataset);
      airlines = io.loadAirlines();

      filtered = logic.filter(flights);
      pivoted = logic.pivot(flights);
      groupedCount = logic.groupCount(flights);
      sorted = logic.sort(flights);
      joined = logic.join(flights, airlines);

      Path cwd = Path.of("").toAbsolutePath().normalize();
      Path repoRoot = cwd.endsWith("java") ? cwd.getParent() : cwd;
      outputDir = repoRoot.resolve("java/out/jmh-write").resolve(flightsDataset);
    }

    public Path nextOutput(String prefix) {
      Path output = outputDir.resolve(prefix + "_" + sequence.incrementAndGet() + ".parquet");
      outputsToCleanup.add(output);
      return output;
    }
  }

  @Setup(Level.Iteration)
  public void setupIteration() {
    realBefore = System.nanoTime();
    cpuBefore = os.getProcessCpuTime();
  }

  @Benchmark
  public Table filter(BenchmarkState state) {
    return state.logic.filter(state.flights);
  }

  @Benchmark
  public Table pivot(BenchmarkState state) {
    return state.logic.pivot(state.flights);
  }

  @Benchmark
  public Table groupCount(BenchmarkState state) {
    return state.logic.groupCount(state.flights);
  }

  @Benchmark
  public Table sort(BenchmarkState state) {
    return state.logic.sort(state.flights);
  }

  @Benchmark
  public Table join(BenchmarkState state) {
    return state.logic.join(state.flights, state.airlines);
  }

  @Benchmark
  public int writeFilter(BenchmarkState state) throws IOException {
    Path output = state.nextOutput("filter");
    state.io.writeParquet(state.filtered, output);
    return state.filtered.rowCount();
  }

  @Benchmark
  public int writePivot(BenchmarkState state) throws IOException {
    Path output = state.nextOutput("pivot");
    state.io.writeParquet(state.pivoted, output);
    return state.pivoted.rowCount();
  }

  @Benchmark
  public int writeGroupCount(BenchmarkState state) throws IOException {
    Path output = state.nextOutput("groupCount");
    state.io.writeParquet(state.groupedCount, output);
    return state.groupedCount.rowCount();
  }

  @Benchmark
  public int writeSort(BenchmarkState state) throws IOException {
    Path output = state.nextOutput("sort");
    state.io.writeParquet(state.sorted, output);
    return state.sorted.rowCount();
  }

  @Benchmark
  public int writeJoin(BenchmarkState state) throws IOException {
    Path output = state.nextOutput("join");
    state.io.writeParquet(state.joined, output);
    return state.joined.rowCount();
  }

  @TearDown(Level.Iteration)
  public void tearDownIteration(ExtraMetrics x, BenchmarkState state) throws IOException {
    long cpuAfter = os.getProcessCpuTime();
    long realAfter = System.nanoTime();

    x.RAM = usedHeapMb() - initUsedMb;
    x.CPU = (double) (cpuAfter - cpuBefore) / (double) (realAfter - realBefore);

    cleanupOutputs(state);
  }

  protected void cleanupOutputs(BenchmarkState state) throws IOException {
    for (Path output : state.outputsToCleanup) {
      Files.deleteIfExists(output);
      Path crcPath = output.resolveSibling("." + output.getFileName() + ".crc");
      Files.deleteIfExists(crcPath);
    }
    state.outputsToCleanup.clear();
  }

  private long usedHeapMb() {
    Runtime runtime = Runtime.getRuntime();
    long usedBytes = runtime.totalMemory() - runtime.freeMemory();
    return usedBytes / MB;
  }
}
