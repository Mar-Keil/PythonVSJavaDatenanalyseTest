package com.tablesaw.benchmark.defaults;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

public class PrintCSV {

  private final Path outputDir;
  private final Path csvPath;

  public PrintCSV() {
    Path cwd = Path.of("").toAbsolutePath().normalize();
    Path repoRoot = cwd.endsWith("java") ? cwd.getParent() : cwd;
    this.outputDir = repoRoot.resolve("java/out/csv");
    this.csvPath = createCsv();
  }

  private Path createCsv() {
    try {
      Files.createDirectories(outputDir);

      int fileNumber = 1;
      Path candidate = outputDir.resolve(fileNumber + "results.csv");

      while (Files.exists(candidate)) {
        fileNumber++;
        candidate = outputDir.resolve(fileNumber + "results.csv");
      }

      Files.writeString(
          candidate,
          "benchmark_size,method,category,score,unit" + System.lineSeparator(),
          StandardCharsets.UTF_8,
          StandardOpenOption.CREATE_NEW);

      return candidate;
    } catch (IOException exception) {
      throw new IllegalStateException("Could not create CSV output file.", exception);
    }
  }
}
