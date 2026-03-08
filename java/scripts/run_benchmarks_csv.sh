#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
JAR_PATH="$ROOT_DIR/target/java-0.1.0-SNAPSHOT-benchmarks.jar"
OUT_DIR="$ROOT_DIR/out/jmh-results"
OUT_CSV="$OUT_DIR/results.csv"
READ_CSV="$OUT_DIR/read_results.csv"
WRITE_CSV="$OUT_DIR/run_write_results.csv"

mkdir -p "$OUT_DIR"

if [[ ! -f "$JAR_PATH" ]]; then
  echo "Missing benchmark jar: $JAR_PATH"
  echo "Build first: mvn clean package"
  exit 1
fi

JAVA_HOME="${JAVA_HOME:-$(/usr/libexec/java_home -v 17)}"
PATH="$JAVA_HOME/bin:$PATH"

java -jar "$JAR_PATH" \
  com.tablesaw.benchmark.ReadBenchmarks \
  -rf csv \
  -rff "$READ_CSV"

java -jar "$JAR_PATH" \
  com.tablesaw.benchmark.RunWriteBenchmarks \
  -rf csv \
  -rff "$WRITE_CSV"

{
  head -n 1 "$READ_CSV"
  tail -n +2 "$READ_CSV"
  tail -n +2 "$WRITE_CSV"
} > "$OUT_CSV"

python3 - <<'PY' "$OUT_CSV"
import csv
import math
import os
import sys
import tempfile

csv_path = sys.argv[1]

with open(csv_path, newline="", encoding="utf-8") as f:
    rows = list(csv.reader(f))

if not rows:
    sys.exit(0)

header = rows[0]
benchmark_idx = header.index("Benchmark")
samples_idx = header.index("Samples")
score_idx = header.index("Score")
unit_idx = header.index("Unit")

def parse_decimal(value: str) -> float:
    return float(value.replace(",", "."))

def format_decimal(value: float) -> str:
    return f"{value:.6f}".replace(".", ",")

filtered_rows = [rows[0]]

for row in rows[1:]:
    benchmark = row[benchmark_idx]

    score_raw = row[score_idx]
    if score_raw == "NaN":
        filtered_rows.append(row)
        continue

    try:
        score = parse_decimal(score_raw)
    except ValueError:
        filtered_rows.append(row)
        continue

    if benchmark.endswith(":CPU"):
        samples_raw = row[samples_idx]
        try:
            samples = float(samples_raw)
        except ValueError:
            filtered_rows.append(row)
            continue

        if math.isfinite(score) and samples > 0:
            row[score_idx] = format_decimal(score / samples)
        row[unit_idx] = "ratio/iter"
        filtered_rows.append(row)
        continue

    if benchmark.endswith(":RAM"):
        samples_raw = row[samples_idx]
        try:
            samples = float(samples_raw)
        except ValueError:
            filtered_rows.append(row)
            continue

        if math.isfinite(score) and samples > 0:
            row[score_idx] = format_decimal(score / samples)
        row[unit_idx] = "MB/iter"

    filtered_rows.append(row)

fd, tmp_path = tempfile.mkstemp(prefix="jmh_results_", suffix=".csv", dir=os.path.dirname(csv_path))
os.close(fd)
with open(tmp_path, "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerows(filtered_rows)

os.replace(tmp_path, csv_path)
PY

rm -f "$READ_CSV" "$WRITE_CSV"

echo "Wrote CSV results to: $OUT_CSV"
