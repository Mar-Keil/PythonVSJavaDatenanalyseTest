package com.tablesaw.io;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import net.tlabs.tablesaw.parquet.TablesawParquetReadOptions;
import net.tlabs.tablesaw.parquet.TablesawParquetReader;
import net.tlabs.tablesaw.parquet.TablesawParquetWriteOptions;
import net.tlabs.tablesaw.parquet.TablesawParquetWriter;
import tech.tablesaw.api.Table;

public class TablesawIO {
  private final Path repoRoot;

  public TablesawIO() {
    Path cwd = Paths.get("").toAbsolutePath().normalize();
    this.repoRoot = cwd.endsWith("java") ? cwd.getParent() : cwd;
  }

  public Table loadFlights() {
    return readParquet(repoRoot.resolve("data-gen/out/flights.parquet"));
  }

  public Table loadFlights(String rows) {
    return readParquet(repoRoot.resolve("data-gen/out").resolve(formatRowsLabel(rows) + "Flights.parquet"));
  }

  public Table loadFlightsByLabel(String datasetLabel) {
    return readParquet(repoRoot.resolve("data-gen/out").resolve(datasetLabel + "Flights.parquet"));
  }

  public Table loadAirlines() {
    return readParquet(repoRoot.resolve("data-gen/out/airlines.parquet"));
  }

  public Table loadAirlines(String rows) {
    return loadAirlines();
  }

  public void writeParquet(Table table, String relativeOutputPath) throws IOException {
    writeParquet(table, repoRoot.resolve(relativeOutputPath));
  }

  public void writeParquet(Table table, Path output) throws IOException {
    Files.createDirectories(output.getParent());
    new TablesawParquetWriter().write(table, TablesawParquetWriteOptions.builder(output.toFile()).build());
  }

  private Table readParquet(Path input) {
    return new TablesawParquetReader().read(TablesawParquetReadOptions.builder(input.toFile()).build());
  }

  private String formatRowsLabel(String rows) {
    long value = Long.parseLong(rows);
    if (value % 1_000_000 == 0) {
      return (value / 1_000_000) + "m";
    }
    if (value % 1_000 == 0) {
      return (value / 1_000) + "k";
    }
    return rows;
  }
}
