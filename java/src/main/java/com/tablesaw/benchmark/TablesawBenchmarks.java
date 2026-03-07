package com.tablesaw.benchmark;

import com.tablesaw.io.TablesawIO;
import com.tablesaw.logic.TablesawLogic;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import tech.tablesaw.api.Table;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 1)
@Measurement(iterations = 2)
@Fork(value = 1)
public class TablesawBenchmarks {
  @State(Scope.Benchmark)
  public static class BaseState {
    @Param({"25k", "100k", "400k", "1600k", "6400k", "25600k"})
    public String flightsDataset;

    protected final TablesawIO io = new TablesawIO();
    protected final TablesawLogic logic = new TablesawLogic();
  }

  @State(Scope.Benchmark)
  public static class DataState extends BaseState {
    protected Table flights;
    protected Table airlines;

    @Setup(Level.Iteration)
    public void setup() {
      flights = io.loadFlightsByLabel(flightsDataset);
      airlines = io.loadAirlines();
    }
  }

  @State(Scope.Benchmark)
  public static class WriteState extends DataState {
    private Table filtered;
    private Table pivoted;
    private Table groupedCount;
    private Table sorted;
    private Table joined;
    private Path outputDir;
    private final AtomicLong sequence = new AtomicLong();

    @Setup(Level.Iteration)
    public void setupWrite() {
      super.setup();
      filtered = logic.filter(flights);
      pivoted = logic.pivot(flights);
      groupedCount = logic.groupCount(flights);
      sorted = logic.sort(flights);
      joined = logic.join(flights, airlines);
      Path cwd = Path.of("").toAbsolutePath().normalize();
      Path repoRoot = cwd.endsWith("java") ? cwd.getParent() : cwd;
      outputDir = repoRoot.resolve("java/out/jmh-write").resolve(flightsDataset);
    }

  }

  @Benchmark
  public int readDatasets(BaseState state) {
    Table flights = state.io.loadFlightsByLabel(state.flightsDataset);
    Table airlines = state.io.loadAirlines();
    return flights.rowCount() + airlines.rowCount();
  }

  @Benchmark
  public Table filter(DataState state) {
    return state.logic.filter(state.flights);
  }

  @Benchmark
  public Table pivot(DataState state) {
    return state.logic.pivot(state.flights);
  }

  @Benchmark
  public Table groupCount(DataState state) {
    return state.logic.groupCount(state.flights);
  }

  @Benchmark
  public Table sort(DataState state) {
    return state.logic.sort(state.flights);
  }

  @Benchmark
  public Table join(DataState state) {
    return state.logic.join(state.flights, state.airlines);
  }

  @Benchmark
  public int writeFilter(WriteState state) throws IOException {
    Path output = state.outputDir.resolve("filter_" + state.sequence.incrementAndGet() + ".parquet");
    state.io.writeParquet(state.filtered, output);
    cleanupWrittenFile(output);
    return state.filtered.rowCount();
  }

  @Benchmark
  public int writePivot(WriteState state) throws IOException {
    Path output = state.outputDir.resolve("pivot_" + state.sequence.incrementAndGet() + ".parquet");
    state.io.writeParquet(state.pivoted, output);
    cleanupWrittenFile(output);
    return state.pivoted.rowCount();
  }

  @Benchmark
  public int writeGroupCount(WriteState state) throws IOException {
    Path output = state.outputDir.resolve("groupCount_" + state.sequence.incrementAndGet() + ".parquet");
    state.io.writeParquet(state.groupedCount, output);
    cleanupWrittenFile(output);
    return state.groupedCount.rowCount();
  }

  @Benchmark
  public int writeSort(WriteState state) throws IOException {
    Path output = state.outputDir.resolve("sort_" + state.sequence.incrementAndGet() + ".parquet");
    state.io.writeParquet(state.sorted, output);
    cleanupWrittenFile(output);
    return state.sorted.rowCount();
  }

  @Benchmark
  public int writeJoin(WriteState state) throws IOException {
    Path output = state.outputDir.resolve("join_" + state.sequence.incrementAndGet() + ".parquet");
    state.io.writeParquet(state.joined, output);
    cleanupWrittenFile(output);
    return state.joined.rowCount();
  }

  private void cleanupWrittenFile(Path output) throws IOException {
    Files.deleteIfExists(output);
    Path crcPath = output.resolveSibling("." + output.getFileName() + ".crc");
    Files.deleteIfExists(crcPath);
  }
}
