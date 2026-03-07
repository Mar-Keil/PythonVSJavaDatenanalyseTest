#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
JAR_PATH="$ROOT_DIR/target/java-0.1.0-SNAPSHOT-benchmarks.jar"
OUT_DIR="$ROOT_DIR/out/jmh-results"
OUT_CSV="$OUT_DIR/results.csv"

mkdir -p "$OUT_DIR"

if [[ ! -f "$JAR_PATH" ]]; then
  echo "Missing benchmark jar: $JAR_PATH"
  echo "Build first: mvn clean package"
  exit 1
fi

JAVA_HOME="${JAVA_HOME:-$(/usr/libexec/java_home -v 17)}"
PATH="$JAVA_HOME/bin:$PATH"

java -jar "$JAR_PATH" \
  com.tablesaw.benchmark.TablesawBenchmarks \
  -rf csv \
  -rff "$OUT_CSV"

echo "Wrote CSV results to: $OUT_CSV"
