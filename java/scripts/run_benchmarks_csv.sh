#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
JAR_PATH="$ROOT_DIR/target/java-0.1.0-SNAPSHOT-benchmarks.jar"
OUT_DIR="$ROOT_DIR/out/jmh-results"

mkdir -p "$OUT_DIR"

RUN_NUMBER=1
while [[ -f "$OUT_DIR/results${RUN_NUMBER}.csv" ]]; do
  RUN_NUMBER=$((RUN_NUMBER + 1))
done

OUT_CSV="$OUT_DIR/results${RUN_NUMBER}.csv"
READ_CSV="$OUT_DIR/read_results_${RUN_NUMBER}.csv"
WRITE_CSV="$OUT_DIR/run_write_results_${RUN_NUMBER}.csv"
NORMALIZER_SCRIPT="$ROOT_DIR/scripts/normalize_jmh_csv.py"

if [[ ! -f "$JAR_PATH" ]]; then
  echo "Missing benchmark jar: $JAR_PATH"
  echo "Build first: mvn clean package"
  exit 1
fi

JAVA_HOME="${JAVA_HOME:-$(/usr/libexec/java_home -v 17)}"
PATH="$JAVA_HOME/bin:$PATH"
JAVA_CMD=("$JAVA_HOME/bin/java")

if [[ -n "${JAVA_XMS:-}" ]]; then
  JAVA_CMD+=("-Xms$JAVA_XMS")
fi

if [[ -n "${JAVA_XMX:-}" ]]; then
  JAVA_CMD+=("-Xmx$JAVA_XMX")
fi

if [[ ${#JAVA_CMD[@]} -gt 1 ]]; then
  echo "Using JVM heap options: ${JAVA_CMD[*]:1}"
fi

"${JAVA_CMD[@]}" -jar "$JAR_PATH" \
  "com.tablesaw.benchmark.read.*" \
  -rf csv \
  -rff "$READ_CSV"

"${JAVA_CMD[@]}" -jar "$JAR_PATH" \
  "com.tablesaw.benchmark.run.*" \
  -rf csv \
  -rff "$WRITE_CSV"

{
  head -n 1 "$READ_CSV"
  tail -n +2 "$READ_CSV"
  tail -n +2 "$WRITE_CSV"
} > "$OUT_CSV"

python3 "$NORMALIZER_SCRIPT" "$OUT_CSV"

rm -f "$READ_CSV" "$WRITE_CSV"

echo "Wrote CSV results to: $OUT_CSV"
