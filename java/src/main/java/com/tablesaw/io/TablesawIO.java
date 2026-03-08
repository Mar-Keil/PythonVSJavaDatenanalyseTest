package com.tablesaw.io;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import net.tlabs.tablesaw.parquet.TablesawParquetReadOptions;
import net.tlabs.tablesaw.parquet.TablesawParquetReader;
import net.tlabs.tablesaw.parquet.TablesawParquetWriteOptions;
import net.tlabs.tablesaw.parquet.TablesawParquetWriter;
import tech.tablesaw.api.Table;

public class TablesawIO {
  public Table readParquet(Path input) {
    return new TablesawParquetReader().read(TablesawParquetReadOptions.builder(input.toFile()).build());
  }

  public void writeParquet(Table table, Path output) throws IOException {
    Files.createDirectories(output.getParent());
    new TablesawParquetWriter().write(table, TablesawParquetWriteOptions.builder(output.toFile()).build());
  }
}
