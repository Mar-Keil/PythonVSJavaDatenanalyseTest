package com.tablesaw.benchmark.defaults;

import com.sun.management.OperatingSystemMXBean;
import com.tablesaw.io.TablesawIO;
import com.tablesaw.logic.TablesawLogic;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.management.ManagementFactory;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
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

  private final long processId;

  private final AtomicLong rssSumMb;
  private final AtomicLong rssSampleCount;
  private final AtomicBoolean rssSamplerRunning;
  private Thread rssSamplerThread;

  private final Path dataOutDir;
  private final Path writeRootDir;

  @Param({"50k", "200k", "800k", "3200k", "12800k"})
  protected String flightsDataset;

  protected BenchmarkDefaults() {
    this.io = new TablesawIO();
    this.logic = new TablesawLogic();
    this.os = (OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();
    this.processId = ProcessHandle.current().pid();
    this.rssSumMb = new AtomicLong();
    this.rssSampleCount = new AtomicLong();
    this.rssSamplerRunning = new AtomicBoolean(false);

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
    rssSumMb.set(0L);
    rssSampleCount.set(0L);

    rssSamplerRunning.set(true);
    rssSamplerThread = createRssSamplerThread();
    rssSamplerThread.start();
    realBefore = System.nanoTime();
    cpuBefore = os.getProcessCpuTime();
  }

  @TearDown(Level.Iteration)
  public void tearDownIterationMetrics(ExtraMetrics metrics) {
    long cpuAfter = os.getProcessCpuTime();
    long realAfter = System.nanoTime();
    stopRssSampler();

    metrics.RAM = rssSumMb.get() / rssSampleCount.get();
    metrics.CPU = (double) (cpuAfter - cpuBefore) / (double) (realAfter - realBefore);
  }

  private Thread createRssSamplerThread() {
    Thread thread = new Thread(
        () -> {
          while (rssSamplerRunning.get()) {
            rssSumMb.addAndGet(readProcessRssMb());
              rssSampleCount.incrementAndGet();

            try {
              Thread.sleep(10L);
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
              return;
            }
          }
        },
        "jmh-rss-sampler");
    thread.setDaemon(true);
    return thread;
  }

  private void stopRssSampler() {
    rssSamplerRunning.set(false);

    if (rssSamplerThread == null) {
      return;
    }

    rssSamplerThread.interrupt();

    try {
      rssSamplerThread.join();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("Interrupted while stopping RSS sampler", e);
    } finally {
      rssSamplerThread = null;
    }
  }

  private long readProcessRssMb() {
    try {
      Process process = new ProcessBuilder("ps", "-o", "rss=", "-p", Long.toString(processId)).start();
      try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()))) {
        String rssLine = reader.readLine();
        int exitCode = process.waitFor();

        if (exitCode != 0 || rssLine == null || rssLine.isBlank()) {
          throw new IllegalStateException("Unable to read RSS for process " + processId);
        }

        return Long.parseLong(rssLine.trim()) / 1024L;
      }
    } catch (IOException | InterruptedException e) {
      if (e instanceof InterruptedException) {
        Thread.currentThread().interrupt();
      }
      throw new IllegalStateException("Failed to read RSS via ps", e);
    }
  }
}
